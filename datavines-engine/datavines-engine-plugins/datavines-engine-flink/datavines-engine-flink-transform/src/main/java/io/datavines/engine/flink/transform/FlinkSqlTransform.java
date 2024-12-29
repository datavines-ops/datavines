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
package io.datavines.engine.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.datavines.common.config.Config;
import io.datavines.common.config.CheckResult;
import io.datavines.engine.api.plugin.Plugin;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.api.component.Component;

public class FlinkSqlTransform implements Plugin, Component {
    
    private String sql;
    private StreamTableEnvironment tableEnv;
    private Config config;
    
    public FlinkSqlTransform(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }
    
    public DataStream<?> transform(DataStream<?> input) {
        // Register input as table
        tableEnv.createTemporaryView("input_table", input);
        
        // Execute SQL transformation
        Table resultTable = tableEnv.sqlQuery(sql);
        
        // Convert back to DataStream
        return tableEnv.toDataStream(resultTable);
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
        this.sql = config.getString("sql");
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        if (sql == null || sql.isEmpty()) {
            return new CheckResult(false, "sql cannot be empty");
        }
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(RuntimeEnvironment env) throws Exception {
        // No preparation needed
    }
}
