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
package io.datavines.engine.flink;

import io.datavines.common.config.Config;
import io.datavines.common.config.Configurations;
import io.datavines.common.entity.JobExecutionRequest;
import io.datavines.common.entity.ProcessResult;

import org.slf4j.Logger;

import io.datavines.common.config.CheckResult;
import io.datavines.engine.api.engine.EngineExecutor;
import io.datavines.engine.flink.config.FlinkEngineConfig;
import io.datavines.engine.flink.executor.FlinkEngineExecutor;

public class FlinkEngine implements EngineExecutor {

    private FlinkEngineConfig flinkEngineConfig;
    private FlinkEngineExecutor executor;

    public FlinkEngine() {
        this.flinkEngineConfig = new FlinkEngineConfig();
        this.executor = new FlinkEngineExecutor();
    }

    public void init(JobExecutionRequest jobExecutionRequest, Logger logger, Configurations configurations) throws Exception {
        executor.init(jobExecutionRequest, logger, configurations);
    }

    public void execute() throws Exception {
        executor.execute();
    }

    public void after() throws Exception {
        executor.after();
    }

    public void cancel() throws Exception {
        executor.cancel();
    }

    public boolean isCancel() throws Exception {
        return executor.isCancel();
    }

    public ProcessResult getProcessResult() {
        return executor.getProcessResult();
    }

    public JobExecutionRequest getTaskRequest() {
        return executor.getTaskRequest();
    }

    public String getName() {
        return "flink";
    }

    public void setConfig(Config config) {
        if (config != null) {
            this.flinkEngineConfig.setConfig(config);
        }
    }

    public Config getConfig() {
        return this.flinkEngineConfig.getConfig();
    }

    public CheckResult checkConfig() {
        return this.flinkEngineConfig.checkConfig();
    }
}
