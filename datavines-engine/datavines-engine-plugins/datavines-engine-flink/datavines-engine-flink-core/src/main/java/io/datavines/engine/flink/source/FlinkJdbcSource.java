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
package io.datavines.engine.flink.core.source;

import io.datavines.common.config.CheckResult;
import io.datavines.common.config.Config;
import io.datavines.common.utils.CryptionUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;
import io.datavines.engine.flink.api.stream.FlinkStreamSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;

import static io.datavines.common.ConfigConstants.*;

public class FlinkJdbcSource implements FlinkStreamSource {

    private static final long serialVersionUID = 1L;

    private Config config = new Config();
    private transient String[] fieldNames;
    private transient Class<?>[] fieldTypes;

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
                    "please specify " + nonExistsOptions.stream().map(option ->
                            "[" + option + "]").collect(Collectors.joining(",")) + " as non-empty string");
        } else {
            return new CheckResult(true, "");
        }
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        String driver = config.getString(DRIVER, "com.mysql.jdbc.Driver");
        Class.forName(driver);

        String url = config.getString(URL);
        String user = config.getString(USER);
        String password = config.getString(PASSWORD);
        String table = config.getString(TABLE);
        String query = config.getString(SQL, "SELECT * FROM " + table);

        if (!StringUtils.isEmptyOrNullStr(password)) {
            try {
                password = CryptionUtils.decryptByAES(password, "datavines");
            } catch (Exception e) {
                throw new RuntimeException("Failed to decrypt password", e);
            }
        }

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            try (java.sql.PreparedStatement ps = conn.prepareStatement(query)) {
                ResultSetMetaData metaData = ps.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                fieldNames = new String[columnCount];
                fieldTypes = new Class<?>[columnCount];
                
                for (int i = 0; i < columnCount; i++) {
                    fieldNames[i] = metaData.getColumnName(i + 1);
                    fieldTypes[i] = Class.forName(metaData.getColumnClassName(i + 1));
                }
            }
        }
    }

    @Override
    public DataStream<Row> getData(FlinkRuntimeEnvironment environment) {
        String url = config.getString(URL);
        String user = config.getString(USER);
        String password = config.getString(PASSWORD);
        String driver = config.getString(DRIVER, "com.mysql.jdbc.Driver");
        String table = config.getString(TABLE);
        String query = config.getString(SQL, "SELECT * FROM " + table);

        if (!StringUtils.isEmptyOrNullStr(password)) {
            try {
                password = CryptionUtils.decryptByAES(password, "datavines");
            } catch (Exception e) {
                throw new RuntimeException("Failed to decrypt password", e);
            }
        }

        TypeInformation<?>[] typeInfos = new TypeInformation[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            typeInfos[i] = TypeInformation.of(fieldTypes[i]);
        }
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInfos, fieldNames);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(password)
                .setQuery(query)
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        return environment.getEnv().createInput(jdbcInputFormat);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public Class<?>[] getFieldTypes() {
        return fieldTypes;
    }
}
