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
package io.datavines.server.api.controller;

import io.datavines.common.entity.job.SubmitJob;
import io.datavines.core.aop.RefreshToken;
import io.datavines.core.constant.DataVinesConstants;
import io.datavines.core.exception.DataVinesServerException;
import io.datavines.server.api.dto.bo.job.JobExecutionDashboardParam;
import io.datavines.server.api.dto.bo.job.JobExecutionPageParam;
import io.datavines.server.api.dto.bo.job.JobQualityReportDashboardParam;
import io.datavines.server.api.dto.vo.JobExecutionResultVO;
import io.datavines.server.repository.entity.JobExecution;
import io.datavines.server.repository.service.JobExecutionErrorDataService;
import io.datavines.server.repository.service.JobExecutionResultService;
import io.datavines.server.repository.service.JobExecutionService;
import io.datavines.server.repository.service.JobQualityReportService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@Slf4j
@Api(value = "job-quality-report", tags = "job-quality-report", produces = MediaType.APPLICATION_JSON_VALUE)
@RestController
@RequestMapping(value = DataVinesConstants.BASE_API_PATH + "/job/quality-report", produces = MediaType.APPLICATION_JSON_VALUE)
@RefreshToken
public class JobQualityReportController {

    @Autowired
    private JobExecutionService jobExecutionService;

    @Autowired
    private JobExecutionResultService jobExecutionResultService;

    @Autowired
    private JobExecutionErrorDataService jobExecutionErrorDataService;

    @Autowired
    private JobQualityReportService jobQualityReportService;

    @ApiOperation(value = "get job quality report page", response = JobExecutionResultVO.class, responseContainer = "page")
    @PostMapping(value = "/page")
    public Object page(@Valid @RequestBody JobQualityReportDashboardParam dashboardParam)  {
        return jobQualityReportService.getQualityReportPage(dashboardParam);
    }

    @ApiOperation(value = "get job quality report page", response = JobExecutionResultVO.class, responseContainer = "list")
    @GetMapping(value = "/listColumnExecution")
    public Object listColumnExecution(@RequestParam Long reportId)  {
        return jobQualityReportService.listColumnExecution(reportId);
    }

    @ApiOperation(value = "get job quality report score", response = JobExecutionResultVO.class)
    @PostMapping(value = "/score")
    public Object getScoreByCondition(@Valid @RequestBody JobQualityReportDashboardParam dashboardParam)  {
        return jobQualityReportService.getScoreByCondition(dashboardParam);
    }

    @ApiOperation(value = "get job quality report score trend", response = JobExecutionResultVO.class)
    @PostMapping(value = "/score-trend")
    public Object getScoreTrendByCondition(@Valid @RequestBody JobQualityReportDashboardParam dashboardParam)  {
        return jobQualityReportService.getScoreTrendByCondition(dashboardParam);
    }
}
