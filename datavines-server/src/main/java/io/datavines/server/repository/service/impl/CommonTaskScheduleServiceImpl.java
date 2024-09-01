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

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.datavines.common.utils.JSONUtils;
import io.datavines.core.enums.Status;
import io.datavines.core.exception.DataVinesServerException;
import io.datavines.server.api.dto.bo.task.CommonTaskScheduleCreateOrUpdate;
import io.datavines.server.api.dto.bo.job.schedule.MapParam;
import io.datavines.server.dqc.coordinator.quartz.CatalogTaskScheduleJob;
import io.datavines.server.dqc.coordinator.quartz.QuartzExecutors;
import io.datavines.server.dqc.coordinator.quartz.ScheduleJobInfo;
import io.datavines.server.dqc.coordinator.quartz.cron.StrategyFactory;
import io.datavines.server.dqc.coordinator.quartz.cron.FunCron;
import io.datavines.server.enums.JobScheduleType;
import io.datavines.server.enums.ScheduleJobType;
import io.datavines.server.repository.entity.DataSource;
import io.datavines.server.repository.entity.CommonTaskSchedule;
import io.datavines.server.repository.mapper.CommonTaskScheduleMapper;
import io.datavines.server.repository.service.CommonTaskScheduleService;
import io.datavines.server.repository.service.DataSourceService;
import io.datavines.server.utils.ContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service("commonTaskScheduleService")
public class CommonTaskScheduleServiceImpl extends ServiceImpl<CommonTaskScheduleMapper, CommonTaskSchedule>  implements CommonTaskScheduleService {

    @Autowired
    private QuartzExecutors quartzExecutor;

    @Autowired
    private DataSourceService dataSourceService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CommonTaskSchedule createOrUpdate(CommonTaskScheduleCreateOrUpdate scheduleCreateOrUpdate) throws DataVinesServerException {
        if (scheduleCreateOrUpdate.getId() != null && scheduleCreateOrUpdate.getId() != 0) {
            return update(scheduleCreateOrUpdate);
        } else {
            return create(scheduleCreateOrUpdate);
        }
    }

    private CommonTaskSchedule create(CommonTaskScheduleCreateOrUpdate scheduleCreateOrUpdate) throws DataVinesServerException {

        Long dataSourceId = scheduleCreateOrUpdate.getDataSourceId();
        CommonTaskSchedule commonTaskSchedule = baseMapper.selectOne(new QueryWrapper<CommonTaskSchedule>().lambda().eq(CommonTaskSchedule::getDataSourceId, dataSourceId).eq(CommonTaskSchedule::getTaskType, scheduleCreateOrUpdate.getTaskType()));
        if (commonTaskSchedule != null) {
            throw new DataVinesServerException(Status.CATALOG_TASK_SCHEDULE_EXIST_ERROR, commonTaskSchedule.getId());
        }

        commonTaskSchedule = new CommonTaskSchedule();
        BeanUtils.copyProperties(scheduleCreateOrUpdate, commonTaskSchedule);
        commonTaskSchedule.setCreateBy(ContextHolder.getUserId());
        commonTaskSchedule.setCreateTime(LocalDateTime.now());
        commonTaskSchedule.setUpdateBy(ContextHolder.getUserId());
        commonTaskSchedule.setUpdateTime(LocalDateTime.now());
        commonTaskSchedule.setStatus(true);

        updateCatalogTaskScheduleParam(commonTaskSchedule, scheduleCreateOrUpdate.getType(), scheduleCreateOrUpdate.getParam());
        DataSource dataSource = dataSourceService.getById(dataSourceId);
        if (dataSource == null) {
            throw new DataVinesServerException(Status.DATASOURCE_NOT_EXIST_ERROR, dataSourceId);
        } else {
            if (baseMapper.insert(commonTaskSchedule) <= 0) {
                log.info("create catalog task schedule fail : {}", commonTaskSchedule);
                throw new DataVinesServerException(Status.CREATE_CATALOG_TASK_SCHEDULE_ERROR);
            }
            try {
                addScheduleJob(scheduleCreateOrUpdate, commonTaskSchedule);
            } catch (Exception e) {
                throw new DataVinesServerException(Status.ADD_QUARTZ_ERROR);
            }
        }
        log.info("create job schedule success: datasource id : {}, cronExpression : {}",
                commonTaskSchedule.getDataSourceId(),
                commonTaskSchedule.getCronExpression());

        return commonTaskSchedule;
    }

    private void addScheduleJob(CommonTaskScheduleCreateOrUpdate scheduleCreateOrUpdate, CommonTaskSchedule commonTaskSchedule) throws ParseException {
        switch (JobScheduleType.of(scheduleCreateOrUpdate.getType())) {
            case CYCLE:
            case CRONTAB:
                quartzExecutor.addJob(CatalogTaskScheduleJob.class, getScheduleJobInfo(commonTaskSchedule));
                break;
            case OFFLINE:
                break;
            default:
                throw new DataVinesServerException(Status.SCHEDULE_TYPE_NOT_VALIDATE_ERROR, scheduleCreateOrUpdate.getType());
        }
    }

    private CommonTaskSchedule update(CommonTaskScheduleCreateOrUpdate scheduleCreateOrUpdate) throws DataVinesServerException {
        CommonTaskSchedule commonTaskSchedule = getById(scheduleCreateOrUpdate.getId());
        if (commonTaskSchedule == null) {
            throw new DataVinesServerException(Status.CATALOG_TASK_SCHEDULE_NOT_EXIST_ERROR, scheduleCreateOrUpdate.getId());
        }

        BeanUtils.copyProperties(scheduleCreateOrUpdate, commonTaskSchedule);
        commonTaskSchedule.setUpdateBy(ContextHolder.getUserId());
        commonTaskSchedule.setUpdateTime(LocalDateTime.now());

        updateCatalogTaskScheduleParam(commonTaskSchedule, scheduleCreateOrUpdate.getType(), scheduleCreateOrUpdate.getParam());

        Long dataSourceId = scheduleCreateOrUpdate.getDataSourceId();
        if (dataSourceId == null) {
            throw new DataVinesServerException(Status.DATASOURCE_NOT_EXIST_ERROR);
        }

        try {
            quartzExecutor.deleteJob(getScheduleJobInfo(commonTaskSchedule));
            addScheduleJob(scheduleCreateOrUpdate, commonTaskSchedule);
        } catch (Exception e) {
            throw new DataVinesServerException(Status.ADD_QUARTZ_ERROR);
        }


        if (baseMapper.updateById(commonTaskSchedule) <= 0) {
            log.info("update catalog task schedule fail : {}", commonTaskSchedule);
            throw new DataVinesServerException(Status.UPDATE_CATALOG_TASK_SCHEDULE_ERROR, commonTaskSchedule.getId());
        }

        return commonTaskSchedule;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean deleteById(long id) {
        CommonTaskSchedule commonTaskSchedule = getById(id);
        if (commonTaskSchedule != null) {
            boolean deleteJob = quartzExecutor.deleteJob(getScheduleJobInfo(commonTaskSchedule));
            if (!deleteJob) {
                return false;
            }
            return removeById(id);
        } else {
            return false;
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean deleteByDataSourceId(long dataSourceId) {
        CommonTaskSchedule commonTaskSchedule = baseMapper.getByDataSourceId(dataSourceId);
        if (commonTaskSchedule == null) {
            return false;
        }

        boolean deleteJob = quartzExecutor.deleteJob(getScheduleJobInfo(commonTaskSchedule));
        if (!deleteJob ) {
            return false;
        }
        removeById(commonTaskSchedule.getId());
        return true;
    }

    @Override
    public CommonTaskSchedule getByDataSourceId(Long dataSourceId, String taskType) {
        return baseMapper.getByDataSourceIdAndType(dataSourceId, taskType);
    }

    @Override
    public CommonTaskSchedule getById(long id) {
        return baseMapper.selectById(id);
    }

    @Override
    public  List<String> getCron(MapParam mapParam){
        List<String> listCron = new ArrayList<>();
        FunCron api = StrategyFactory.getByType(mapParam.getCycle());
        CommonTaskSchedule commonTaskSchedule = new CommonTaskSchedule();
        String result1 = JSONUtils.toJsonString(mapParam);
        commonTaskSchedule.setParam(result1);
        String cron = api.funcDeal(commonTaskSchedule.getParam());
        listCron.add(cron);
        return listCron;
    }

    private void updateCatalogTaskScheduleParam(CommonTaskSchedule commonTaskSchedule, String type, MapParam param) {
        String paramStr = JSONUtils.toJsonString(param);
        switch (JobScheduleType.of(type)){
            case CYCLE:
                if (param == null) {
                    throw new DataVinesServerException(Status.SCHEDULE_PARAMETER_IS_NULL_ERROR);
                }

                if (param.getCycle() == null) {
                    throw new DataVinesServerException(Status.SCHEDULE_PARAMETER_IS_NULL_ERROR);
                }
                commonTaskSchedule.setStatus(true);
                commonTaskSchedule.setParam(paramStr);
                FunCron api = StrategyFactory.getByType(param.getCycle());
                commonTaskSchedule.setCronExpression(api.funcDeal(commonTaskSchedule.getParam()));

                log.info("job schedule param: {}", paramStr);
                break;
            case CRONTAB:
                if (param == null) {
                    throw new DataVinesServerException(Status.SCHEDULE_PARAMETER_IS_NULL_ERROR);
                }

                boolean isValid = quartzExecutor.isValid(param.getCrontab());
                if (!isValid) {
                    throw new DataVinesServerException(Status.SCHEDULE_CRON_IS_INVALID_ERROR, param.getCrontab());
                }
                commonTaskSchedule.setStatus(true);
                commonTaskSchedule.setParam(paramStr);
                commonTaskSchedule.setCronExpression(param.getCrontab());
                break;
            case OFFLINE:
                commonTaskSchedule.setStatus(false);
                break;
            default:
                throw new DataVinesServerException(Status.SCHEDULE_TYPE_NOT_VALIDATE_ERROR, type);
        }
    }

    private ScheduleJobInfo getScheduleJobInfo(CommonTaskSchedule commonTaskSchedule) {
        return new ScheduleJobInfo(
                ScheduleJobType.CATALOG,
                commonTaskSchedule.getTaskType(),
                commonTaskSchedule.getDataSourceId(),
                commonTaskSchedule.getId(),
                commonTaskSchedule.getCronExpression(),
                commonTaskSchedule.getStartTime(),
                commonTaskSchedule.getEndTime());
    }
}
