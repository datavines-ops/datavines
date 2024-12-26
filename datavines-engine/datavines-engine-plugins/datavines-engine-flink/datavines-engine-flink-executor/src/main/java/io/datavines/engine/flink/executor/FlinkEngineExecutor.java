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
package io.datavines.engine.flink.executor;

import org.slf4j.Logger;

import io.datavines.common.config.Configurations;
import io.datavines.common.entity.JobExecutionRequest;
import io.datavines.common.entity.ProcessResult;
import io.datavines.common.enums.ExecutionStatus;
import io.datavines.common.utils.LoggerUtils;
import io.datavines.engine.executor.core.base.AbstractYarnEngineExecutor;
import io.datavines.engine.executor.core.executor.ShellCommandProcess;
import io.datavines.engine.flink.executor.utils.FlinkArgsUtils;
import io.datavines.engine.flink.executor.utils.FlinkParameters;

public class FlinkEngineExecutor extends AbstractYarnEngineExecutor {

    private static final String FLINK_COMMAND = "${FLINK_HOME}/bin/flink";
    private Configurations configurations;

    @Override
    public void init(JobExecutionRequest jobExecutionRequest, Logger logger, Configurations configurations) throws Exception {
        String threadLoggerInfoName = String.format(LoggerUtils.JOB_LOG_INFO_FORMAT, jobExecutionRequest.getJobExecutionUniqueId());
        Thread.currentThread().setName(threadLoggerInfoName);

        this.jobExecutionRequest = jobExecutionRequest;
        this.logger = logger;
        this.configurations = configurations;
        this.processResult = new ProcessResult();
        this.shellCommandProcess = new ShellCommandProcess(
            this::logHandle,
            logger,
            jobExecutionRequest,
            configurations
        );
    }

    @Override
    public void execute() throws Exception {
        try {
            String command = buildCommand();
            logger.info("flink task command: {}", command);
            shellCommandProcess.run(command);
            processResult.setExitStatusCode(ExecutionStatus.SUCCESS.getCode());
        } catch (Exception e) {
            logger.error("flink task error", e);
            processResult.setExitStatusCode(ExecutionStatus.FAILURE.getCode());
            throw e;
        }
    }

    @Override
    public void after() throws Exception {
        // 执行后的清理工作
    }

    @Override
    public ProcessResult getProcessResult() {
        return this.processResult;
    }

    @Override
    public JobExecutionRequest getTaskRequest() {
        return this.jobExecutionRequest;
    }

    @Override
    protected String buildCommand() {
        FlinkParameters parameters = new FlinkParameters();
        // Set parameters from configurations
        parameters.setMainJar(configurations.getString("mainJar"));
        parameters.setMainClass(configurations.getString("mainClass"));
        parameters.setDeployMode(configurations.getString("deployMode", "cluster"));
        parameters.setMainArgs(configurations.getString("mainArgs"));
        parameters.setParallelism(configurations.getInt("parallelism", 1));
        parameters.setJobName(configurations.getString("jobName"));
        parameters.setYarnQueue(configurations.getString("yarnQueue"));

        return FLINK_COMMAND + " " + String.join(" ", FlinkArgsUtils.buildArgs(parameters));
    }
}
