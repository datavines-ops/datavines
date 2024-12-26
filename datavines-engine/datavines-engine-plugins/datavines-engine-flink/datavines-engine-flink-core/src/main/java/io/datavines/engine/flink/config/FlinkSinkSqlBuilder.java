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
package io.datavines.engine.flink.core.config;

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
                "  cast(null as string) as create_time,\n" +
                "  cast(null as string) as update_time\n" +
                "from ${table_name}";
    }

    public static String getDefaultSinkSql() {
        return "select\n" +
                "  '${job_execution_id}' as job_execution_id,\n" +
                "  '${metric_unique_key}' as metric_unique_key,\n" +
                "  '${unique_code}' as unique_code,\n" +
                "  ${actual_value} as actual_value,\n" +
                "  ${expected_value} as expected_value,\n" +
                "  '${operator}' as operator,\n" +
                "  '${threshold}' as threshold,\n" +
                "  '${check_type}' as check_type,\n" +
                "  cast(null as string) as create_time,\n" +
                "  cast(null as string) as update_time\n" +
                "from ${table_name} full join ${expected_table}";
    }
}
