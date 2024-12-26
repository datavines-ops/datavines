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
package io.datavines.engine.flink.core.transform;

import io.datavines.common.config.CheckResult;
import io.datavines.common.config.Config;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;
import io.datavines.engine.flink.api.stream.FlinkStreamTransform;
import io.datavines.engine.api.plugin.Plugin;
import io.datavines.common.utils.StringUtils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static io.datavines.common.ConfigConstants.SQL;

public class FlinkSqlTransform implements FlinkStreamTransform, Plugin {
    
    private String sql;
    private String[] outputFieldNames;
    private Class<?>[] outputFieldTypes;
    private Config config;
    
    @Override
    public void setConfig(Config config) {
        this.config = config;
        if (config != null) {
            this.sql = config.getString(SQL);
        }
    }
    
    @Override
    public Config getConfig() {
        return this.config;
    }
    
    @Override
    public CheckResult checkConfig() {
        if (StringUtils.isEmptyOrNullStr(sql)) {
            return new CheckResult(false, "please specify [sql] as non-empty string");
        }
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        // No special preparation needed for SQL transform
    }
    
    @Override
    public DataStream<Row> process(DataStream<Row> dataStream, FlinkRuntimeEnvironment environment) {
        StreamTableEnvironment tableEnv = environment.getTableEnv();
        
        // Register input table
        tableEnv.createTemporaryView("input_table", dataStream);
        
        // Execute SQL transformation
        Table resultTable = tableEnv.sqlQuery(sql);
        
        // Convert back to DataStream
        return tableEnv.toDataStream(resultTable, Row.class);
    }
    
    @Override
    public String[] getOutputFieldNames() {
        return outputFieldNames;
    }
    
    @Override
    public Class<?>[] getOutputFieldTypes() {
        return outputFieldTypes;
    }
    
    public void setSql(String sql) {
        this.sql = sql;
    }
    
    public void setOutputFieldNames(String[] outputFieldNames) {
        this.outputFieldNames = outputFieldNames;
    }
    
    public void setOutputFieldTypes(Class<?>[] outputFieldTypes) {
        this.outputFieldTypes = outputFieldTypes;
    }
}
