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

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.datavines.common.enums.OperatorType;
import io.datavines.common.exception.DataVinesException;
import io.datavines.common.utils.DateUtils;
import io.datavines.common.utils.ParameterUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.core.utils.LanguageUtils;
import io.datavines.metric.api.*;
import io.datavines.server.api.dto.bo.job.JobQualityReportDashboardParam;
import io.datavines.server.api.dto.vo.*;
import io.datavines.server.enums.DataQualityLevel;
import io.datavines.server.enums.DqJobExecutionState;
import io.datavines.server.repository.entity.*;
import io.datavines.server.repository.mapper.JobExecutionResultMapper;
import io.datavines.server.repository.mapper.JobQualityReportMapper;
import io.datavines.server.repository.service.JobExecutionService;
import io.datavines.server.repository.service.JobQualityReportService;
import io.datavines.server.repository.service.JobExecutionResultReportRelService;
import io.datavines.spi.PluginLoader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static io.datavines.common.ConfigConstants.*;

@Service("jobQualityReportService")
public class JobQualityReportServiceImpl extends ServiceImpl<JobQualityReportMapper, JobQualityReport>  implements JobQualityReportService {

    @Autowired
    private JobExecutionResultMapper jobExecutionResultMapper;

    @Autowired
    private JobExecutionResultReportRelService jobExecutionResultReportRelService;

    @Autowired
    private JobQualityReportMapper jobQualityReportMapper;

    @Autowired
    private JobExecutionService jobExecutionService;

    @Transactional(rollbackFor =  Exception.class)
    @Override
    public boolean generateQualityReport(Long datasourceId) {
        String yesterday = DateUtils.format(DateUtils.addDays(DateUtils.getCurrentDate(),-1),DateUtils.YYYY_MM_DD);
        List<JobExecutionResult> jobExecutionResultList = jobExecutionResultMapper.listByDatasourceIdAndTimeRange(datasourceId,yesterday + " 00:00:00", yesterday + " 23:59:59");
        if (CollectionUtils.isEmpty(jobExecutionResultList)) {
            return true;
        }
        Map<String, Set<String>> key2MetricMap = new HashMap<>();
        Map<String, List<Long>> key2IdMap = new HashMap<>();
        Map<String, BigDecimal> key2ScoreMap = new HashMap<>();
        for (JobExecutionResult executionResult : jobExecutionResultList) {
            String key = null;
            if (StringUtils.isEmpty(executionResult.getColumnName())) {
                key = String.format("%s@#@%s",executionResult.getDatabaseName(), executionResult.getTableName());
            } else {
                key = String.format("%s@#@%s@#@%s",executionResult.getDatabaseName(), executionResult.getTableName(), executionResult.getColumnName());
            }

            Set<String> metricSet = key2MetricMap.get(key);
            if (CollectionUtils.isEmpty(metricSet)) {
                metricSet = new HashSet<>();
            }

            if (metricSet.contains(executionResult.getMetricName())) {
                continue;
            }

            List<Long> resultIds = key2IdMap.get(key);
            if (CollectionUtils.isEmpty(resultIds)) {
                resultIds = new ArrayList<>();
            }

            resultIds.add(executionResult.getId());
            key2IdMap.put(key, resultIds);

            BigDecimal scoreSum = key2ScoreMap.get(key);
            if (scoreSum == null) {
                scoreSum = new BigDecimal(0);
            }

            scoreSum = scoreSum.add(executionResult.getScore());
            key2ScoreMap.put(key, scoreSum);

            metricSet.add(executionResult.getMetricName());
            key2MetricMap.put(key,metricSet);
        }

        if (MapUtils.isNotEmpty(key2ScoreMap)) {
            for (Map.Entry<String,BigDecimal> entry : key2ScoreMap.entrySet()) {
                String key = entry.getKey();
                BigDecimal score = entry.getValue();

                List<Long> executionResultIds = key2IdMap.get(key);

                String databaseName = "--";
                String tableName = "--";
                String columnName = "--";
                String[] keyValues = key.split("@#@");
                if (keyValues.length == 2) {
                    databaseName = keyValues[0];
                    tableName = keyValues[1];
                } else if (keyValues.length == 3) {
                    databaseName = keyValues[0];
                    tableName = keyValues[1];
                    columnName = keyValues[2];
                } else {
                    continue;
                }

                JobQualityReport jobQualityReport = null;
                LambdaQueryWrapper<JobQualityReport> queryWrapper = new LambdaQueryWrapper<>();
                queryWrapper.eq(JobQualityReport::getDatasourceId, datasourceId);
                queryWrapper.eq(JobQualityReport::getDatabaseName, databaseName);
                queryWrapper.eq(JobQualityReport::getTableName, tableName);
                queryWrapper.eq(JobQualityReport::getColumnName, columnName);
                queryWrapper.eq(JobQualityReport::getReportDate, yesterday);
                jobQualityReport = jobQualityReportMapper.selectOne(queryWrapper);

                if (jobQualityReport == null) {
                    jobQualityReport = new JobQualityReport();
                    jobQualityReport.setReportDate(LocalDate.parse(yesterday));
                    jobQualityReport.setScore(CollectionUtils.isEmpty(executionResultIds) ? new BigDecimal(0) : score.divide(new BigDecimal(executionResultIds.size()),4, RoundingMode.UP));
                    jobQualityReport.setDatabaseName(databaseName);
                    jobQualityReport.setTableName(tableName);
                    jobQualityReport.setColumnName(columnName);
                    jobQualityReport.setDatasourceId(datasourceId);
                    jobQualityReport.setCreateTime(LocalDateTime.now());
                    jobQualityReport.setUpdateTime(LocalDateTime.now());
                    jobQualityReport.setEntityLevel(MetricLevel.COLUMN.getDescription());
                    jobQualityReportMapper.insert(jobQualityReport);
                } else {
                    jobQualityReport.setScore(CollectionUtils.isEmpty(executionResultIds) ? new BigDecimal(0) : score.divide(new BigDecimal(executionResultIds.size()),4, RoundingMode.UP));
                    jobQualityReport.setUpdateTime(LocalDateTime.now());
                    jobQualityReportMapper.updateById(jobQualityReport);
                }

                Long qualityReportId = jobQualityReport.getId();

                List<JobExecutionResultReportRel> relList = new ArrayList<>();
                for (Long executionResultId : executionResultIds) {
                    JobExecutionResultReportRel rel = new JobExecutionResultReportRel();
                    rel.setQualityReportId(qualityReportId);
                    rel.setJobExecutionResultId(executionResultId);
                    relList.add(rel);
                }

                if (CollectionUtils.isNotEmpty(relList)) {
                    jobExecutionResultReportRelService.remove(new LambdaQueryWrapper<JobExecutionResultReportRel>()
                            .eq(JobExecutionResultReportRel::getQualityReportId,qualityReportId));
                    jobExecutionResultReportRelService.saveBatch(relList);
                }
            }
        }

        // 删除datasource_id 下所有表级别的评分
        jobQualityReportMapper.delete(new LambdaQueryWrapper<JobQualityReport>()
                .eq(JobQualityReport::getEntityLevel, TABLE)
                .eq(JobQualityReport::getDatasourceId, datasourceId)
                .eq(JobQualityReport::getReportDate, yesterday));
        // 根据datasource_id,database_name,table_name 进行group by,计算得到表的分数
        List<JobQualityReport> tableScoreList = jobQualityReportMapper.listTableScoreGroupByDatasource(datasourceId, yesterday);
        if (CollectionUtils.isNotEmpty(tableScoreList)) {
            saveBatch(tableScoreList);
        }

        jobQualityReportMapper.delete(new LambdaQueryWrapper<JobQualityReport>()
                .eq(JobQualityReport::getEntityLevel, DATABASE)
                .eq(JobQualityReport::getDatasourceId, datasourceId)
                .eq(JobQualityReport::getReportDate, yesterday));
        // 根据datasource_id,database_name 进行 group by,计算得到数据库的分数
        List<JobQualityReport> databaseScoreList = jobQualityReportMapper.listDbScoreGroupByDatasource(datasourceId, yesterday);
        if (CollectionUtils.isNotEmpty(databaseScoreList)) {
            saveBatch(databaseScoreList);
        }

        jobQualityReportMapper.delete(new LambdaQueryWrapper<JobQualityReport>()
                .eq(JobQualityReport::getEntityLevel, DATASOURCE)
                .eq(JobQualityReport::getDatasourceId, datasourceId)
                .eq(JobQualityReport::getReportDate, yesterday));;

        // 根据datasource_id,database_name 进行 group by,计算得到数据库的分数
        List<JobQualityReport> datasourceScoreList = jobQualityReportMapper.listDatasourceScoreGroupByDatasource(datasourceId, yesterday);
        if (CollectionUtils.isNotEmpty(datasourceScoreList)) {
            saveBatch(datasourceScoreList);
        }

        return true;
    }

    @Override
    public JobQualityReportScore getScoreByCondition(JobQualityReportDashboardParam dashboardParam) {

        LambdaQueryWrapper<JobQualityReport> queryWrapper = new LambdaQueryWrapper<>();
        if (dashboardParam == null) {
            throw new DataVinesException("param can not be null");
        }

        queryWrapper.eq(dashboardParam.getDatasourceId()!= null, JobQualityReport::getDatasourceId, dashboardParam.getDatasourceId());

        queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getSchemaName()), JobQualityReport::getDatabaseName, dashboardParam.getSchemaName());
        if (StringUtils.isEmpty(dashboardParam.getTableName())) {
            queryWrapper.eq(JobQualityReport::getTableName, "--");
        } else {
            queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getTableName()), JobQualityReport::getTableName, dashboardParam.getTableName());
        }

        if (StringUtils.isEmpty(dashboardParam.getReportDate())) {
            String yesterday = DateUtils.format(DateUtils.addDays(DateUtils.getCurrentDate(),-1),DateUtils.YYYY_MM_DD);
            queryWrapper.eq(JobQualityReport::getReportDate, yesterday);
        } else {
            queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getReportDate()), JobQualityReport::getReportDate, dashboardParam.getReportDate());
        }

        List<JobQualityReport> jobQualityReports = jobQualityReportMapper.selectList(queryWrapper);
        if (CollectionUtils.isEmpty(jobQualityReports)) {
            return null;
        }
        JobQualityReportScore reportScore = new JobQualityReportScore();
        reportScore.setScore(new BigDecimal(0));
        reportScore.setQualityLevel(DataQualityLevel.UNQUALIFIED.getZhDescription());

        JobQualityReport report = jobQualityReports.get(0);
        reportScore.setScore(report.getScore());
        reportScore.setQualityLevel(DataQualityLevel.getQualityLevelByScore(report.getScore()).getZhDescription());
        return reportScore;
    }

    @Override
    public JobQualityReportScoreTrend getScoreTrendByCondition(JobQualityReportDashboardParam dashboardParam) {
        if (dashboardParam == null) {
            throw new DataVinesException("param can not be null");
        }

        JobQualityReportScoreTrend scoreTrend = new JobQualityReportScoreTrend();
        String startDateStr = "";
        String endDateStr = "";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(YYYY_MM_DD);
        if (StringUtils.isEmpty(dashboardParam.getReportDate())) {
            startDateStr = DateUtils.format(DateUtils.addDays(new Date(), -7),YYYY_MM_DD);
            endDateStr = DateUtils.format(DateUtils.addDays(new Date(), -1),YYYY_MM_DD);
        } else {
            endDateStr = dashboardParam.getReportDate();
            LocalDate endDate = LocalDate.parse(endDateStr, formatter);
            ZonedDateTime zonedDateTime = endDate.atStartOfDay(ZoneId.systemDefault());
            Date date = Date.from(zonedDateTime.toInstant());
            startDateStr = DateUtils.format(DateUtils.addDays(date,-6),YYYY_MM_DD);
        }

        LocalDate startDate = LocalDate.parse(startDateStr, formatter);
        LocalDate endDate = LocalDate.parse(endDateStr, formatter);

        List<String> dateList = new ArrayList<>();
        LocalDate currentDate = startDate;

        while (!currentDate.isAfter(endDate)) {
            dateList.add(currentDate.format(DateTimeFormatter.ofPattern(YYYY_MM_DD)));
            currentDate = currentDate.plusDays(1);
        }

        LambdaQueryWrapper<JobQualityReport> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(dashboardParam.getDatasourceId()!= null, JobQualityReport::getDatasourceId, dashboardParam.getDatasourceId());

        if (StringUtils.isEmpty(dashboardParam.getTableName())) {
            queryWrapper.eq(JobQualityReport::getDatabaseName, "--");
        } else {
            queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getSchemaName()), JobQualityReport::getDatabaseName, dashboardParam.getSchemaName());
        }

        if (StringUtils.isEmpty(dashboardParam.getTableName())) {
            queryWrapper.eq(JobQualityReport::getTableName, "--");
        } else {
            queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getTableName()), JobQualityReport::getTableName, dashboardParam.getTableName());
        }

        queryWrapper.between(JobQualityReport::getReportDate, startDateStr, endDateStr);
        queryWrapper.orderByAsc(JobQualityReport::getReportDate);
        List<JobQualityReport> reportList = list(queryWrapper);

        Map<String, BigDecimal> date2Score = new HashMap<>();
        if (CollectionUtils.isNotEmpty(reportList)) {
            reportList.forEach(it -> {
                date2Score.put(it.getReportDate().toString(), it.getScore());
            });
        } else {
            return scoreTrend;
        }

        List<BigDecimal> scoreList = new ArrayList<>();

        dateList.forEach(date -> {
            BigDecimal score = date2Score.get(date);
            if (score == null) {
                scoreList.add(BigDecimal.valueOf(0));
            } else {
                scoreList.add(score);
            }
        });

        scoreTrend.setDateList(dateList);
        scoreTrend.setScoreList(scoreList);

        return scoreTrend;
    }

    @Override
    public IPage<JobQualityReportVO> getQualityReportPage(JobQualityReportDashboardParam dashboardParam) {
        Page<JobQualityReport> page = new Page<>(dashboardParam.getPageNumber(), dashboardParam.getPageSize());
        LambdaQueryWrapper<JobQualityReport> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(JobQualityReport::getDatasourceId, dashboardParam.getDatasourceId());
        queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getSchemaName()),JobQualityReport::getDatabaseName,dashboardParam.getSchemaName());
        queryWrapper.eq(StringUtils.isNotEmpty(dashboardParam.getTableName()),JobQualityReport::getTableName,dashboardParam.getTableName());
        if (StringUtils.isEmpty(dashboardParam.getReportDate())) {
            String yesterday = DateUtils.format(DateUtils.addDays(DateUtils.getCurrentDate(),-1),DateUtils.YYYY_MM_DD);
            queryWrapper.eq(JobQualityReport::getReportDate, yesterday);
        } else {
            queryWrapper.eq(JobQualityReport::getReportDate, dashboardParam.getReportDate());
        }

        if (StringUtils.isNotEmpty(dashboardParam.getTableName())) {
            queryWrapper.eq(JobQualityReport::getEntityLevel, COLUMN);
        } else {
            queryWrapper.eq(JobQualityReport::getEntityLevel, TABLE);
        }

        return page(page, queryWrapper).convert(jobQualityReport -> {
            JobQualityReportVO jobQualityReportVO = new JobQualityReportVO();
            BeanUtils.copyProperties(jobQualityReport, jobQualityReportVO);
            return jobQualityReportVO;
        });
    }

    @Override
    public List<JobExecutionResultVO> listColumnExecution(Long reportId) {
        List<JobExecutionResult> executionResults = jobExecutionResultReportRelService.listExecutionResultByReportId(reportId);
        if (CollectionUtils.isEmpty(executionResults)) {
            return Collections.emptyList();
        }

        return executionResults.stream().map(jobExecutionResult -> {
            Map<String,String> parameters = new HashMap<>();
            parameters.put(ACTUAL_VALUE, String.valueOf(jobExecutionResult.getActualValue()));
            parameters.put(EXPECTED_VALUE, String.valueOf(jobExecutionResult.getExpectedValue()));
            parameters.put(THRESHOLD, String.valueOf(jobExecutionResult.getThreshold()));
            parameters.put(OPERATOR,OperatorType.of(jobExecutionResult.getOperator()).getSymbol());
            JobExecution jobExecution = jobExecutionService.getById(jobExecutionResult.getJobExecutionId());
            JobExecutionResultVO jobExecutionResultVO = new JobExecutionResultVO();
            ResultFormula resultFormula =
                    PluginLoader.getPluginLoader(ResultFormula.class).getOrCreatePlugin(jobExecutionResult.getResultFormula());
            String resultFormulaFormat = resultFormula.getResultFormat(!LanguageUtils.isZhContext())+" ${operator} ${threshold}";

            jobExecutionResultVO.setCheckSubject(jobExecutionResult.getDatabaseName() + "." + jobExecutionResult.getTableName() + "." + jobExecutionResult.getColumnName());
            jobExecutionResultVO.setCheckResult(DqJobExecutionState.of(jobExecutionResult.getState()).getDescription(!LanguageUtils.isZhContext()));
            SqlMetric sqlMetric = PluginLoader.getPluginLoader(SqlMetric.class).getOrCreatePlugin(jobExecutionResult.getMetricName());
            if (!"multi_table_value_comparison".equalsIgnoreCase(sqlMetric.getName())) {
                ExpectedValue expectedValue = PluginLoader.getPluginLoader(ExpectedValue.class).getOrCreatePlugin(jobExecution.getEngineType() + "_" + jobExecutionResult.getExpectedType());
                jobExecutionResultVO.setExpectedType(expectedValue.getNameByLanguage(!LanguageUtils.isZhContext()));
            }
            jobExecutionResultVO.setMetricName(sqlMetric.getNameByLanguage(!LanguageUtils.isZhContext()));

            jobExecutionResultVO.setResultFormulaFormat(ParameterUtils.convertParameterPlaceholders(resultFormulaFormat, parameters));
            jobExecutionResultVO.setScore(jobExecutionResult.getScore());
            jobExecutionResultVO.setExecutionTime(jobExecutionResult.getCreateTime());
            return jobExecutionResultVO;
        }).collect(Collectors.toList());
    }

}
