/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.server.repository.service.impl;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import io.datavines.common.entity.JobExecutionParameter;
import io.datavines.common.utils.DateUtils;
import io.datavines.core.enums.Status;
import io.datavines.core.utils.LanguageUtils;
import io.datavines.metric.api.ResultFormula;
import io.datavines.common.entity.job.SubmitJob;
import io.datavines.server.api.dto.bo.job.JobExecutionDashboardParam;
import io.datavines.server.api.dto.bo.job.JobExecutionPageParam;
import io.datavines.server.api.dto.vo.*;
import io.datavines.core.exception.DataVinesServerException;
import io.datavines.server.enums.DqJobExecutionState;
import io.datavines.server.repository.entity.JobExecution;
import io.datavines.server.repository.entity.JobExecutionResult;
import io.datavines.server.repository.mapper.JobExecutionResultMapper;
import io.datavines.server.repository.service.*;
import io.datavines.server.repository.entity.Command;
import io.datavines.server.repository.mapper.JobExecutionMapper;
import io.datavines.spi.PluginLoader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import io.datavines.common.enums.ExecutionStatus;
import io.datavines.common.utils.JSONUtils;
import io.datavines.server.enums.CommandType;
import io.datavines.server.enums.Priority;
import org.springframework.transaction.annotation.Transactional;

import static io.datavines.common.CommonConstants.YYYY_MM_DD_HH_MM_SS;
import static io.datavines.core.constant.DataVinesConstants.SPARK;

@Service("jobExecutionService")
public class JobExecutionServiceImpl extends ServiceImpl<JobExecutionMapper, JobExecution> implements JobExecutionService {

    @Autowired
    private CommandService commandService;

    @Autowired
    private JobExecutionResultService jobExecutionResultService;

    @Autowired
    private JobExecutionResultMapper jobExecutionResultMapper;

    @Autowired
    private ActualValuesService actualValuesService;

    @Override
    public long create(JobExecution jobExecution) {
        baseMapper.insert(jobExecution);
        return jobExecution.getId();
    }

    @Override
    public int update(JobExecution jobExecution) {
        jobExecution.setUpdateTime(LocalDateTime.now());
        return baseMapper.updateById(jobExecution);
    }

    @Override
    public JobExecution getById(long id) {
        return baseMapper.selectById(id);
    }

    @Override
    public List<JobExecution> listByJobId(long jobId) {
        return baseMapper.listByJobId(jobId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public int deleteByJobId(long jobId) {
        List<JobExecution> jobExecutionList = listByJobId(jobId);
        if (CollectionUtils.isEmpty(jobExecutionList)) {
            return 0;
        }

        jobExecutionList.forEach(execution -> {
            removeById(execution.getId());
            jobExecutionResultService.deleteByJobExecutionId(execution.getId());
            actualValuesService.deleteByJobExecutionId(execution.getId());
        });

        return 0;
    }

    @Override
    public IPage<JobExecutionVO> getJobExecutionPage(JobExecutionPageParam pageParam) {
        Page<JobExecutionVO> page = new Page<>(pageParam.getPageNumber(), pageParam.getPageSize());
        IPage<JobExecutionVO> jobExecutionPage = baseMapper.getJobExecutionPage(page, pageParam.getSearchVal(), pageParam.getJobId(), pageParam.getDatasourceId(), pageParam.getStatus(), pageParam.getMetricType(), pageParam.getSchemaName(), pageParam.getTableName(), pageParam.getColumnName(), pageParam.getStartTime(), pageParam.getEndTime(),
                pageParam.getSchemaSearch(), pageParam.getTableSearch(), pageParam.getColumnSearch());
        List<JobExecutionVO> jobExecutionList = jobExecutionPage.getRecords();
        // get jobExecution checkState separately
        if(CollectionUtils.isNotEmpty(jobExecutionList)){
            List<Long> jobExecutionIdList = jobExecutionList.stream()
                    .filter(r -> r != null && r.getId() != null)
                    .map(JobExecutionVO::getId).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(jobExecutionIdList)) {
                List<JobExecutionAggState> jobExecutionAggStateList = jobExecutionResultMapper.listByJobExecutionId(jobExecutionIdList);
                Map<Long, Integer> jobExecutionStateMap = jobExecutionAggStateList.stream()
                        .collect(Collectors.toMap(
                                JobExecutionAggState::getJobExecutionId,
                                JobExecutionAggState::getCheckState
                        ));
                jobExecutionList.forEach(jobExecution -> jobExecution.setCheckState(DqJobExecutionState.of(jobExecutionStateMap.get(jobExecution.getId()))));
            }
        }
        return jobExecutionPage;
    }

    @Override
    public Long submitJob(SubmitJob submitJob) throws DataVinesServerException {

        checkJobExecutionParameter(submitJob.getParameter(), submitJob.getEngineType());

        JobExecution jobExecution = new JobExecution();
        BeanUtils.copyProperties(submitJob, jobExecution);
        jobExecution.setParameter(JSONUtils.toJsonString(submitJob.getParameter()));
        if (submitJob.getExecutePlatformParameter() != null) {
            jobExecution.setExecutePlatformParameter(JSONUtils.toJsonString(submitJob.getExecutePlatformParameter()));
        }

        if (SPARK.equals(jobExecution.getEngineType())) {
            Map<String, Object> defaultEngineParameter = new HashMap<>();
            defaultEngineParameter.put("programType", "JAVA");
            defaultEngineParameter.put("deployMode", "cluster");
            defaultEngineParameter.put("driverCores", 1);
            defaultEngineParameter.put("driverMemory", "512M");
            defaultEngineParameter.put("numExecutors", 2);
            defaultEngineParameter.put("executorMemory", "2G");
            defaultEngineParameter.put("executorCores", 2);
            defaultEngineParameter.put("others", "--conf spark.yarn.maxAppAttempts=1");

            if (submitJob.getEngineParameter() != null) {
                defaultEngineParameter.putAll(submitJob.getEngineParameter());
            }
            submitJob.setEngineParameter(defaultEngineParameter);
            jobExecution.setEngineParameter(JSONUtils.toJsonString(submitJob.getEngineParameter()));
        }

        jobExecution.setSubmitTime(LocalDateTime.now());
        jobExecution.setStatus(ExecutionStatus.SUBMITTED_SUCCESS);

        return executeJob(jobExecution);
    }

    @Override
    public Long executeJob(JobExecution jobExecution) throws DataVinesServerException {
        Long jobExecutionId = create(jobExecution);

        Map<String, String> parameter = new HashMap<>();
        parameter.put("engine", jobExecution.getEngineType());
        // add a command
        Command command = new Command();
        command.setType(CommandType.START);
        command.setPriority(Priority.MEDIUM);
        command.setParameter(JSONUtils.toJsonString(parameter));
        command.setJobExecutionId(jobExecutionId);
        commandService.insert(command);

        return jobExecutionId;
    }

    @Override
    public Long killJob(Long jobExecutionId) {
        JobExecution jobExecution = getById(jobExecutionId);
        if (jobExecution == null) {
            return jobExecutionId;
        }

        Command command = new Command();
        Map<String, String> parameter = new HashMap<>();
        parameter.put("engine", jobExecution.getEngineType());

        command.setType(CommandType.STOP);
        command.setPriority(Priority.MEDIUM);
        command.setParameter(JSONUtils.toJsonString(parameter));
        command.setJobExecutionId(jobExecutionId);
        commandService.insert(command);

        return jobExecutionId;
    }

    @Override
    public List<JobExecution> listNeedFailover(String host) {
        return baseMapper.selectList(new QueryWrapper<JobExecution>().lambda()
                .eq(JobExecution::getExecuteHost, host)
                .in(JobExecution::getStatus, ExecutionStatus.RUNNING_EXECUTION.getCode(), ExecutionStatus.SUBMITTED_SUCCESS.getCode()));
    }

    @Override
    public List<JobExecution> listJobExecutionNotInServerList(List<String> hostList) {
        return baseMapper.selectList(new QueryWrapper<JobExecution>().lambda()
                .notIn(JobExecution::getExecuteHost, hostList)
                .in(JobExecution::getStatus, ExecutionStatus.RUNNING_EXECUTION.getCode(), ExecutionStatus.SUBMITTED_SUCCESS.getCode()));
    }

    private void checkJobExecutionParameter(JobExecutionParameter jobExecutionParameter, String engineType) throws DataVinesServerException {
//        String metricType = jobExecutionParameter.getMetricType();
//        Set<String> metricPluginSet = PluginLoader.getPluginLoader(SqlMetric.class).getSupportedPlugins();
//        if (!metricPluginSet.contains(metricType)) {
//            throw new DataVinesServerException(String.format("%s metric does not supported", metricType));
//        }
//
//        SqlMetric sqlMetric = PluginLoader.getPluginLoader(SqlMetric.class).getOrCreatePlugin(metricType);
//        CheckResult checkResult = sqlMetric.validateConfig(jobExecutionParameter.getMetricParameter());
//        if (checkResult== null || !checkResult.isSuccess()) {
//            throw new DataVinesServerException(checkResult== null? "check error": checkResult.getMsg());
//        }
//
//        String configBuilder = engineType + "_" + sqlMetric.getType().getDescription();
//        Set<String> configBuilderPluginSet = PluginLoader.getPluginLoader(JobConfigurationBuilder.class).getSupportedPlugins();
//        if (!configBuilderPluginSet.contains(configBuilder)) {
//            throw new DataVinesServerException(String.format("%s engine does not supported %s metric", engineType, metricType));
//        }
//
//        ConnectorParameter connectorParameter = jobExecutionParameter.getConnectorParameter();
//        if (connectorParameter != null) {
//            String connectorType = connectorParameter.getType();
//            Set<String> connectorFactoryPluginSet =
//                    PluginLoader.getPluginLoader(ConnectorFactory.class).getSupportedPlugins();
//            if (!connectorFactoryPluginSet.contains(connectorType)) {
//                throw new DataVinesServerException(String.format("%s connector does not supported", connectorType));
//            }
//
//            if (LOCAL.equals(engineType)) {
//                ConnectorFactory connectorFactory = PluginLoader.getPluginLoader(ConnectorFactory.class).getOrCreatePlugin(connectorType);
//                if (!JDBC.equals(connectorFactory.getCategory())) {
//                    throw new DataVinesServerException(String.format("jdbc engine does not supported %s connector", connectorType));
//                }
//            }
//        } else {
//            throw new DataVinesServerException("connector parameter should not be null");
//        }
//
//        String expectedMetric = jobExecutionParameter.getExpectedType();
//        Set<String> expectedValuePluginSet = PluginLoader.getPluginLoader(ExpectedValue.class).getSupportedPlugins();
//        if (!expectedValuePluginSet.contains(expectedMetric)) {
//            throw new DataVinesServerException(String.format("%s expected value does not supported", metricType));
//        }
//
//        String resultFormula = jobExecutionParameter.getResultFormula();
//        Set<String> resultFormulaPluginSet = PluginLoader.getPluginLoader(ResultFormula.class).getSupportedPlugins();
//        if (!resultFormulaPluginSet.contains(resultFormula)) {
//            throw new DataVinesServerException(String.format("%s result formula does not supported", metricType));
//        }
    }


    /**
     * get task host from jobExecutionId
     *
     * @param jobExecutionId
     */
    @Override
    public String getJobExecutionHost(Long jobExecutionId) {
        JobExecution jobExecution = baseMapper.selectById(jobExecutionId);
        if (null == jobExecution) {
            throw new DataVinesServerException(Status.TASK_NOT_EXIST_ERROR, jobExecutionId);
        }
        if (jobExecution.getStatus() == ExecutionStatus.SUBMITTED_SUCCESS) {
            throw new DataVinesServerException(Status.TASK_EXECUTE_NOT_RUNNING, jobExecutionId);
        }
        String executeHost = jobExecution.getExecuteHost();
        if (StringUtils.isEmpty(executeHost)) {
            throw new DataVinesServerException(Status.TASK_EXECUTE_HOST_NOT_EXIST_ERROR, jobExecutionId);
        }
        return executeHost;
    }

    @Override
    public List<MetricExecutionDashBoard> getMetricExecutionDashBoard(Long jobId, String startTime, String endTime) {

        List<MetricExecutionDashBoard> resultList = new ArrayList<>();

        List<JobExecutionResult> executionResults = jobExecutionResultService.listByJobIdAndTimeRange(jobId, startTime, endTime);
        if (CollectionUtils.isEmpty(executionResults)) {
            return resultList;
        }

        executionResults.forEach(result -> {
            ResultFormula resultFormula =
                    PluginLoader.getPluginLoader(ResultFormula.class).getOrCreatePlugin(result.getResultFormula());
            MetricExecutionDashBoard executionDashBoard = new MetricExecutionDashBoard();
            executionDashBoard.setValue(resultFormula.getResult(result.getActualValue(), Objects.isNull(result.getExpectedValue()) ? 0 : result.getExpectedValue()));
            executionDashBoard.setType(resultFormula.getType().getDescription());
            executionDashBoard.setDatetime(result.getCreateTime().toString());

            resultList.add(executionDashBoard);
        });

        return resultList;
    }

    @Override
    public List<JobExecutionAggItem> getJobExecutionAggPie(JobExecutionDashboardParam dashboardParam) {
        List<String> statusList = new ArrayList<>(Arrays.asList("6", "7"));

        String startDateStr = "";
        String endDateStr = "";
        if (StringUtils.isEmpty(dashboardParam.getStartTime()) && StringUtils.isEmpty(dashboardParam.getEndTime())) {
            startDateStr = DateUtils.format(DateUtils.addDays(new Date(), -5), "yyyy-MM-dd");
            endDateStr = DateUtils.format(DateUtils.addDays(new Date(), +1), "yyyy-MM-dd");
        } else {
            if (StringUtils.isEmpty(dashboardParam.getEndTime()) && StringUtils.isNotEmpty(dashboardParam.getStartTime())) {
                startDateStr = dashboardParam.getStartTime().substring(0, 10);
                Date startDate = DateUtils.stringToDate(dashboardParam.getStartTime());
                endDateStr = DateUtils.format(DateUtils.addDays(startDate, 7), "yyyy-MM-dd");
            } else if (StringUtils.isEmpty(dashboardParam.getStartTime()) && StringUtils.isNotEmpty(dashboardParam.getEndTime())) {
                endDateStr = dashboardParam.getEndTime().substring(0, 10);
                Date endDate = DateUtils.stringToDate(dashboardParam.getEndTime());
                startDateStr = DateUtils.format(DateUtils.addDays(endDate, -6), "yyyy-MM-dd");
            } else {
                Date endDate = DateUtils.parse(dashboardParam.getEndTime(), YYYY_MM_DD_HH_MM_SS);
                Date startDate = DateUtils.parse(dashboardParam.getStartTime(), YYYY_MM_DD_HH_MM_SS);
                long days = DateUtils.diffDays(endDate, startDate);
                if (days > 7) {
                    endDate = DateUtils.addDays(startDate, 7);
                }
                startDateStr = DateUtils.format(startDate, "yyyy-MM-dd");
                endDateStr = DateUtils.format(endDate, "yyyy-MM-dd");
            }
        }
        startDateStr += " 00:00:00";
        endDateStr += " 23:59:59";

        List<JobExecutionAggItem> items =
                baseMapper.getJobExecutionAggPie(dashboardParam.getDatasourceId(), dashboardParam.getMetricType(),
                        dashboardParam.getSchemaName(), dashboardParam.getTableName(), dashboardParam.getColumnName(),
                        startDateStr, endDateStr);

        if (CollectionUtils.isEmpty(items)) {
            return new ArrayList<>();
        }
        items = items.stream().filter(it -> statusList.contains(it.getName())).collect(Collectors.toList());
        Set<String> resultStatusSet = items.stream().map(JobExecutionAggItem::getName).collect(Collectors.toSet());

        for (String status : statusList) {
            if (!resultStatusSet.contains(status)) {
                JobExecutionAggItem item = new JobExecutionAggItem();
                item.setName(status);
                item.setValue(0);
                items.add(item);
            }
        }
        items = items.stream().sorted(Comparator.comparing(JobExecutionAggItem::getName)).collect(Collectors.toList());
        boolean isZh = LanguageUtils.isZhContext();
        for (JobExecutionAggItem jobExecutionAggItem : items) {
            switch (jobExecutionAggItem.getName()) {
                case "6":
                    if (isZh) {
                        jobExecutionAggItem.setName("执行失败");
                    } else {
                        jobExecutionAggItem.setName("Failure");
                    }
                    break;
                case "7":
                    if (isZh) {
                        jobExecutionAggItem.setName("执行成功");
                    } else {
                        jobExecutionAggItem.setName("Success");
                    }
                    break;
                default:
                    break;
            }
        }

        return items;
    }

    @Override
    public JobExecutionTrendBar getJobExecutionTrendBar(JobExecutionDashboardParam dashboardParam) {

        JobExecutionTrendBar trendBar = new JobExecutionTrendBar();

        String startDateStr = "";
        String endDateStr = "";
        if (StringUtils.isEmpty(dashboardParam.getStartTime()) && StringUtils.isEmpty(dashboardParam.getEndTime())) {
            startDateStr = DateUtils.format(DateUtils.addDays(new Date(), -5), "yyyy-MM-dd");
            endDateStr = DateUtils.format(DateUtils.addDays(new Date(), +1), "yyyy-MM-dd");
        } else {
            if (StringUtils.isEmpty(dashboardParam.getEndTime()) && StringUtils.isNotEmpty(dashboardParam.getStartTime())) {
                startDateStr = dashboardParam.getStartTime().substring(0, 10);
                Date startDate = DateUtils.stringToDate(dashboardParam.getStartTime());
                endDateStr = DateUtils.format(DateUtils.addDays(startDate, 7), "yyyy-MM-dd");
            } else if (StringUtils.isEmpty(dashboardParam.getStartTime()) && StringUtils.isNotEmpty(dashboardParam.getEndTime())) {
                endDateStr = dashboardParam.getEndTime().substring(0, 10);
                Date endDate = DateUtils.stringToDate(dashboardParam.getEndTime());
                startDateStr = DateUtils.format(DateUtils.addDays(endDate, -6), "yyyy-MM-dd");
            } else {
                Date endDate = DateUtils.parse(dashboardParam.getEndTime(), YYYY_MM_DD_HH_MM_SS);
                Date startDate = DateUtils.parse(dashboardParam.getStartTime(), YYYY_MM_DD_HH_MM_SS);
                long days = DateUtils.diffDays(endDate, startDate);
                if (days > 7) {
                    endDate = DateUtils.addDays(startDate, 7);
                }
                startDateStr = DateUtils.format(startDate, "yyyy-MM-dd");
                endDateStr = DateUtils.format(endDate, "yyyy-MM-dd");
            }
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startDate = LocalDate.parse(startDateStr, formatter);
        LocalDate endDate = LocalDate.parse(endDateStr, formatter);

        List<String> dateList = new ArrayList<>();
        LocalDate currentDate = startDate;

        while (!currentDate.isAfter(endDate)) {
            dateList.add(currentDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            currentDate = currentDate.plusDays(1);
        }

        startDateStr += " 00:00:00";
        endDateStr += " 23:59:59";
        List<JobExecutionTrendBarItem> trendBars = baseMapper.getJobExecutionTrendBar(dashboardParam.getDatasourceId(),
                dashboardParam.getMetricType(), dashboardParam.getSchemaName(), dashboardParam.getTableName(), dashboardParam.getColumnName(),
                startDateStr, endDateStr);

        Map<String, List<JobExecutionTrendBarItem>> trendBarListMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(trendBars)) {
            trendBars.forEach(it -> {
                if (trendBarListMap.get(it.getCreateDate()) == null) {
                    List<JobExecutionTrendBarItem> list = new ArrayList<>();
                    list.add(it);
                    trendBarListMap.put(it.getCreateDate(), list);
                } else {
                    trendBarListMap.get(it.getCreateDate()).add(it);
                }
            });
        } else {
            return null;
        }

        List<Integer> allList = new ArrayList<>();
        List<Integer> successList = new ArrayList<>();
        List<Integer> failureList = new ArrayList<>();

        dateList.forEach(date -> {
            List<JobExecutionTrendBarItem> list = trendBarListMap.get(date);
            if (CollectionUtils.isEmpty(list)) {
                allList.add(0);
                successList.add(0);
                failureList.add(0);
            } else {
                int success = 0;
                int failure = 0;
                for (JobExecutionTrendBarItem item : list) {
                    if (item.getStatus() == 6) {
                        failure += item.getNum();
                    } else if (item.getStatus() == 7) {
                        success += item.getNum();
                    }
                }
                allList.add(failure + success);
                failureList.add(failure);
                successList.add(success);
            }
        });

        trendBar.setDateList(dateList);
        trendBar.setAllList(allList);
        trendBar.setSuccessList(successList);
        trendBar.setFailureList(failureList);

        return trendBar;
    }

    @Override
    public JobExecutionStat getJobExecutionStat(Long jobId) {
        return baseMapper.getJobExecutionStat(jobId);
    }
}
