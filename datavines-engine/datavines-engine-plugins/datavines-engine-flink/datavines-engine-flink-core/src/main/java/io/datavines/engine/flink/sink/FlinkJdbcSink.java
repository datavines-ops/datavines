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
package io.datavines.engine.flink.core.sink;

import io.datavines.common.config.CheckResult;
import io.datavines.common.config.Config;
import io.datavines.common.utils.StringUtils;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;
import io.datavines.engine.flink.api.stream.FlinkStreamSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;
import static io.datavines.common.ConfigConstants.*;

import io.datavines.engine.common.utils.ParserUtils;

public class FlinkJdbcSink implements FlinkStreamSink {

    private static final long serialVersionUID = 1L;

    private Config config = new Config();
    private transient String[] fieldNames;
    private transient int batchSize = 1000;
    private transient long batchIntervalMs = 200;
    private transient int maxRetries = 3;

    @Override
    public void setConfig(Config config) {
        if(config != null) {
            this.config = config;
            this.batchSize = config.getInt("jdbc.batch.size", 1000);
            this.batchIntervalMs = config.getLong("jdbc.batch.interval.ms", 200L);
            this.maxRetries = config.getInt("jdbc.max.retries", 3);
        }
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        List<String> requiredOptions = Arrays.asList(URL, TABLE, USER, PASSWORD);

        List<String> nonExistsOptions = new ArrayList<>();
        requiredOptions.forEach(x->{
            if(!config.has(x)){
                nonExistsOptions.add(x);
            }
        });

        if (!nonExistsOptions.isEmpty()) {
            return new CheckResult(
                    false,
                    "please specify " + nonExistsOptions.stream().map(option ->
                            "[" + option + "]").collect(Collectors.joining(",")) + " as non-empty string");
        } else {
            return new CheckResult(true, "");
        }
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        // Load JDBC driver class
        String driver = config.getString(DRIVER, "com.mysql.jdbc.Driver");
        Class.forName(driver);

        // Get table metadata to initialize field names
        String url = config.getString(URL);
        String user = config.getString(USER);
        String password = config.getString(PASSWORD);
        String table = config.getString(TABLE);

        if (!StringUtils.isEmptyOrNullStr(password)) {
            password = ParserUtils.decode(password);
        }

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + table + " WHERE 1=0")) {
                ResultSetMetaData metaData = ps.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                fieldNames = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    fieldNames[i] = metaData.getColumnName(i + 1);
                }
            }
        }
    }

    @Override
    public void output(DataStream<Row> dataStream, FlinkRuntimeEnvironment environment) {
        String url = config.getString(URL);
        String table = config.getString(TABLE);
        String user = config.getString(USER);
        String password = config.getString(PASSWORD);
        String driver = config.getString(DRIVER, "com.mysql.jdbc.Driver");

        // Decode password if needed
        if (!StringUtils.isEmptyOrNullStr(password)) {
            password = ParserUtils.decode(password);
        }

        // Build JDBC execution options
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(batchIntervalMs)
                .withMaxRetries(maxRetries)
                .build();

        // Create insert SQL statement
        String insertSql = createInsertSql(table, fieldNames);

        // Build JDBC sink
        dataStream.addSink(JdbcSink.sink(
                insertSql,
                (statement, row) -> {
                    for (int i = 0; i < fieldNames.length; i++) {
                        statement.setObject(i + 1, row.getField(i));
                    }
                },
                executionOptions,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        ));
    }

    private String createInsertSql(String table, String[] fieldNames) {
        String columns = String.join(", ", fieldNames);
        String placeholders = String.join(", ", Collections.nCopies(fieldNames.length, "?"));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders);
    }
}
