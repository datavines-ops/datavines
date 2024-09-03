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

import io.datavines.core.constant.DataVinesConstants;
import io.datavines.core.entity.ResultMap;
import io.datavines.core.enums.Status;
import io.datavines.core.exception.DataVinesServerException;
import io.datavines.server.api.annotation.AuthIgnore;
import io.datavines.server.dqc.coordinator.log.LogService;
import io.datavines.server.repository.entity.JobExecution;
import io.datavines.server.repository.service.JobExecutionService;
import io.datavines.server.utils.FileUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static io.datavines.common.utils.OSUtils.judgeConcurrentHost;


@Slf4j
@Api(value = "job", tags = "job", produces = MediaType.APPLICATION_JSON_VALUE)
@RestController
@RequestMapping(value = DataVinesConstants.BASE_API_PATH + "/history/job/execution", produces = MediaType.APPLICATION_JSON_VALUE)
public class JobHistoryExecutionController {

    @Autowired
    private JobExecutionService jobExecutionService;

    @Resource
    private LogService logService;


    @AuthIgnore
    @ApiOperation(value = "queryLogWithOffsetLine", notes = "query task log with offsetLine")
    @GetMapping(value = "/queryLogWithOffsetLine")
    public Object queryLogWithOffsetLine(@RequestParam("taskId") Long taskId,
                                         @RequestParam("offsetLine") int offsetLine,
                                         HttpServletRequest request, HttpServletResponse response) throws IOException {
        String taskHost = jobExecutionService.getJobExecutionHost(taskId);
        Boolean isConcurrentHost = judgeConcurrentHost(taskHost);
        if (isConcurrentHost) {
            return ResponseEntity.ok(new ResultMap().success().payload(logService.queryLog(taskId, offsetLine)));
        }

        response.sendRedirect(request.getScheme() + "://" + taskHost +
                "/api/v1/history/job/execution/queryLogWithOffsetLine?offsetLine=" + offsetLine + "&taskId=" + taskId + "&Authorization=" + request.getHeader("Authorization"));
        return null;
    }

    @AuthIgnore
    @ApiOperation(value = "download", notes = "download log file")
    @GetMapping(value = "/download")
    public void download(@RequestParam("taskId") Long taskId, HttpServletRequest request, HttpServletResponse response) throws IOException {
        JobExecution jobExecution = jobExecutionService.getById(taskId);
        if(null == jobExecution){
            throw new DataVinesServerException(Status.TASK_NOT_EXIST_ERROR, taskId);
        }
        String taskHost = jobExecution.getExecuteHost();
        if(StringUtils.isEmpty(taskHost)){
            throw new DataVinesServerException(Status.TASK_EXECUTE_HOST_NOT_EXIST_ERROR, taskId);
        }
        Boolean isConcurrentHost = judgeConcurrentHost(taskHost);
        if (isConcurrentHost) {
            if(StringUtils.isEmpty(jobExecution.getLogPath())){
                throw new DataVinesServerException(Status.TASK_LOG_PATH_NOT_EXIST_ERROR, taskId);
            }
            FileUtils.downloadToResp(jobExecution.getLogPath(), response);
            return;
        }
        response.sendRedirect(request.getScheme() + "://" + taskHost + "/api/v1/history/job/execution/download?taskId=" + taskId+"&Authorization="+request.getHeader("Authorization"));
    }


}
