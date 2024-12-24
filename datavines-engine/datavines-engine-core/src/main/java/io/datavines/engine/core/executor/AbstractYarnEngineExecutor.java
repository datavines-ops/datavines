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
package io.datavines.engine.core.executor;

import org.slf4j.Logger;

import io.datavines.common.config.Configurations;
import io.datavines.common.entity.JobExecutionRequest;
import io.datavines.common.entity.ProcessResult;
import io.datavines.engine.api.engine.EngineExecutor;
import io.datavines.engine.core.utils.ShellCommandProcess;

public abstract class AbstractYarnEngineExecutor implements EngineExecutor {

    protected JobExecutionRequest jobExecutionRequest;
    protected Logger logger;
    protected ShellCommandProcess shellCommandProcess;

    @Override
    public void init(JobExecutionRequest jobExecutionRequest, Logger logger, Configurations configurations) throws Exception {
        this.jobExecutionRequest = jobExecutionRequest;
        this.logger = logger;
    }

    @Override
    public abstract void execute() throws Exception;

    @Override
    public abstract void after() throws Exception;

    @Override
    public abstract void cancel() throws Exception;

    @Override
    public abstract boolean isCancel() throws Exception;

    @Override
    public abstract ProcessResult getProcessResult();

    @Override
    public JobExecutionRequest getTaskRequest() {
        return jobExecutionRequest;
    }

    protected void logHandle(String log) {
        // Subclasses can override this method to handle logs differently
        logger.info(log);
    }
}
