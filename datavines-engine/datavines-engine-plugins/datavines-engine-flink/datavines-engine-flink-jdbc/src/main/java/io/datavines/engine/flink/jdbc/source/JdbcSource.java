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
package io.datavines.engine.flink.jdbc.source;

import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.types.Row;

import io.datavines.common.config.Config;
import io.datavines.common.config.CheckResult;
import io.datavines.engine.api.plugin.Plugin;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.api.component.Component;

public class JdbcSource implements Plugin, Component {
    
    private String driverName;
    private String jdbcUrl;
    private String username;
    private String password;
    private String query;
    private Config config;
    
    public JdbcInputFormat getSource() {
        return JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(driverName)
                .setDBUrl(jdbcUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setRowTypeInfo(null) // Need to be implemented based on actual schema
                .finish();
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
        this.driverName = config.getString("driverName");
        this.jdbcUrl = config.getString("jdbcUrl");
        this.username = config.getString("username");
        this.password = config.getString("password");
        this.query = config.getString("query");
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        if (driverName == null || driverName.isEmpty()) {
            return new CheckResult(false, "driverName cannot be empty");
        }
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return new CheckResult(false, "jdbcUrl cannot be empty");
        }
        if (username == null || username.isEmpty()) {
            return new CheckResult(false, "username cannot be empty");
        }
        if (password == null || password.isEmpty()) {
            return new CheckResult(false, "password cannot be empty");
        }
        if (query == null || query.isEmpty()) {
            return new CheckResult(false, "query cannot be empty");
        }
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        // Load JDBC driver
        Class.forName(driverName);
    }
}
