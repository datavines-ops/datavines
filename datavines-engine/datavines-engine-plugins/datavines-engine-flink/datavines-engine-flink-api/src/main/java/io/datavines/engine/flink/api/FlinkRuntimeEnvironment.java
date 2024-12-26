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
package io.datavines.engine.flink.api;

import io.datavines.common.config.CheckResult;
import io.datavines.common.config.Config;
import io.datavines.common.exception.DataVinesException;
import io.datavines.engine.api.env.Execution;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.flink.api.stream.FlinkStreamExecution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkRuntimeEnvironment implements RuntimeEnvironment {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;
    private Config config;
    private FlinkStreamExecution execution;

    public FlinkRuntimeEnvironment() {
        this.config = new Config();
        this.execution = new FlinkStreamExecution(this);
    }

    public void setConfig(Config config) {
        if (config != null) {
            this.config = config;
        }
    }

    public Config getConfig() {
        return config;
    }

    public CheckResult checkConfig() {
        return new CheckResult(true, "Configuration check passed");
    }

    public Execution getExecution() {
        return execution;
    }

    public void prepare() {
        try {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            tableEnv = StreamTableEnvironment.create(env);
        } catch (Exception e) {
            throw new DataVinesException("Failed to prepare Flink environment", e);
        }
    }

    public void stop() {
        try {
            if (env != null) {
                // Flink's environment doesn't have a direct cancel method, 
                // we need to handle job cancellation through JobClient
            }
        } catch (Exception e) {
            throw new DataVinesException("Failed to stop Flink environment", e);
        }
    }

    public String getType() {
        return "flink";
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }
}
