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

public class FlinkSinkSqlBuilder {

    private FlinkSinkSqlBuilder() {
        throw new IllegalStateException("Utility class");
    }

    public static String getActualValueSql() {
        return "select\n" +
                "  '${job_execution_id}' as job_execution_id,\n" +
                "  '${metric_unique_key}' as metric_unique_key,\n" +
                "  '${unique_code}' as unique_code,\n" +
                "  ${actual_value} as actual_value,\n" +
                "  cast(null as string) as expected_value,\n" +
                "  cast(null as string) as operator,\n" +
                "  cast(null as string) as threshold,\n" +
                "  cast(null as string) as check_type,\n" +
                "  CURRENT_TIMESTAMP as create_time,\n" +
                "  CURRENT_TIMESTAMP as update_time\n" +
                "from ${table_name}";
    }

    public static String getDefaultSinkSql() {
        return "select\n" +
                "  '${job_execution_id}' as job_execution_id,\n" +
                "  '${metric_unique_key}' as metric_unique_key,\n" +
                "  '${unique_code}' as unique_code,\n" +
                "  CASE WHEN ${actual_value} IS NULL THEN NULL ELSE ${actual_value} END as actual_value,\n" +
                "  CASE WHEN ${expected_value} IS NULL THEN NULL ELSE ${expected_value} END as expected_value,\n" +
                "  '${metric_type}' as metric_type,\n" +
                "  '${metric_name}' as metric_name,\n" +
                "  '${metric_dimension}' as metric_dimension,\n" +
                "  '${database_name}' as database_name,\n" +
                "  '${table_name}' as table_name,\n" +
                "  '${column_name}' as column_name,\n" +
                "  '${operator}' as operator,\n" +
                "  '${threshold}' as threshold,\n" +
                "  '${expected_type}' as expected_type,\n" +
                "  '${result_formula}' as result_formula,\n" +
                "  CASE WHEN ${actual_value} IS NULL THEN '${default_state}' ELSE NULL END as state,\n" +
                "  CURRENT_TIMESTAMP as create_time,\n" +
                "  CURRENT_TIMESTAMP as update_time\n" +
                "from ${table_name} full join ${expected_table}";
    }
}
