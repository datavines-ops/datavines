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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.stream.Collectors;

import io.datavines.common.config.Config;
import io.datavines.common.config.CheckResult;
import io.datavines.common.utils.StringUtils;
import io.datavines.common.utils.TypesafeConfigUtils;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;
import io.datavines.engine.flink.api.stream.FlinkStreamSource;
import static io.datavines.common.ConfigConstants.*;

public class JdbcSource implements FlinkStreamSource {
    
    private Config config = new Config();
    
    private JdbcInputFormat buildJdbcInputFormat() {
        Properties properties = new Properties();
        properties.setProperty(USER, config.getString(USER));
        properties.setProperty(DRIVER, config.getString(DRIVER));
        String password = config.getString(PASSWORD);
        if (!StringUtils.isEmptyOrNullStr(password)) {
            properties.put(PASSWORD, password);
        }

        Config jdbcConfig = TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false);
        if (!jdbcConfig.isEmpty()) {
            jdbcConfig.entrySet().forEach(x -> {
                properties.put(x.getKey(), String.valueOf(x.getValue()));
            });
        }

        return JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(properties.getProperty(DRIVER))
                .setDBUrl(config.getString(URL))
                .setUsername(properties.getProperty(USER))
                .setPassword(properties.getProperty(PASSWORD))
                .setQuery(config.getString(TABLE))
                .setRowTypeInfo(null) // Need to be implemented based on actual schema
                .finish();
    }

    @Override
    public DataStream<Row> getData(FlinkRuntimeEnvironment environment) {
        return environment.getEnv()
                .createInput(buildJdbcInputFormat());
    }

    @Override
    public String[] getFieldNames() {
        // TODO: Implement this based on database metadata
        return new String[0];
    }

    @Override
    public Class<?>[] getFieldTypes() {
        // TODO: Implement this based on database metadata
        return new Class<?>[0];
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
        List<String> requiredOptions = Arrays.asList(URL, TABLE, USER);

        List<String> nonExistsOptions = new ArrayList<>();
        requiredOptions.forEach(x->{
            if(!config.has(x)){
                nonExistsOptions.add(x);
            }
        });

        if (!nonExistsOptions.isEmpty()) {
            return new CheckResult(
                    false,
                    "please specify " + String.join(",", nonExistsOptions.stream()
                            .map(option -> "[" + option + "]")
                            .collect(Collectors.toList())) + " as non-empty string");
        } else {
            return new CheckResult(true, "");
        }
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'prepare'");
    }
}
