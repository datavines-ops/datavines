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
package io.datavines.engine.flink.jdbc.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.datavines.common.config.Config;
import io.datavines.common.config.CheckResult;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;
import io.datavines.engine.flink.api.stream.FlinkStreamSink;

public class JdbcSink implements FlinkStreamSink {
    
    private String driverName;
    private String jdbcUrl;
    private String username;
    private String password;
    private String query;
    private Config config = new Config();

    @Override
    public void output(DataStream<Row> dataStream, FlinkRuntimeEnvironment environment) {
        dataStream.addSink(org.apache.flink.connector.jdbc.JdbcSink.sink(
            query,
            (statement, row) -> {
                // Need to be implemented based on actual schema
                for (int i = 0; i < row.getArity(); i++) {
                    statement.setObject(i + 1, row.getField(i));
                }
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName(driverName)
                .withUsername(username)
                .withPassword(password)
                .build()
        ));
    }

    @Override
    public void setConfig(Config config) {
        if(config != null) {
            this.config = config;
        }
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        if (config != null) {
            this.driverName = config.getString("driver");
            this.jdbcUrl = config.getString("url");
            this.username = config.getString("username");
            this.password = config.getString("password");
            this.query = config.getString("query");
        }
    }
}
