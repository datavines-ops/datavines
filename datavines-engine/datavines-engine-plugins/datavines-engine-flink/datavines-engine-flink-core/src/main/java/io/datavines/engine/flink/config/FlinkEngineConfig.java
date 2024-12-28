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
package io.datavines.engine.flink.config;

import io.datavines.common.config.Config;
import io.datavines.common.config.CheckResult;
import io.datavines.engine.api.plugin.Plugin;
import org.apache.flink.api.common.RuntimeExecutionMode;

import java.io.Serializable;

public class FlinkEngineConfig implements Plugin, Serializable {
    
    private static final long serialVersionUID = 1L;

    private static final String CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
    private static final String PARALLELISM = "flink.parallelism";
    private static final String RESTART_ATTEMPTS = "flink.restart.attempts";
    private static final String RESTART_DELAY = "flink.restart.delay";
    private static final String STATE_BACKEND = "flink.state.backend";
    private static final String CHECKPOINT_PATH = "flink.checkpoint.path";
    private static final String EXECUTION_MODE = "flink.execution.mode";
    
    private Config config;
    
    public FlinkEngineConfig() {
        this.config = new Config();
    }

    @Override
    public void setConfig(Config config) {
        this.config = config != null ? config : new Config();
    }
    
    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }
    
    public long getCheckpointInterval() {
        return config.getLong(CHECKPOINT_INTERVAL, 10000L);
    }
    
    public int getParallelism() {
        return config.getInt(PARALLELISM, 1);
    }
    
    public int getRestartAttempts() {
        return config.getInt(RESTART_ATTEMPTS, 3);
    }
    
    public long getRestartDelay() {
        return config.getLong(RESTART_DELAY, 10000L);
    }
    
    public String getStateBackend() {
        return config.getString(STATE_BACKEND, "memory");
    }
    
    public String getCheckpointPath() {
        return config.getString(CHECKPOINT_PATH, "");
    }

    public RuntimeExecutionMode getExecutionMode() {
        String mode = config.getString(EXECUTION_MODE, "STREAMING");
        return RuntimeExecutionMode.valueOf(mode.toUpperCase());
    }
}
