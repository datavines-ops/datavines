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
package io.datavines.metric.expected.plugin;

import io.datavines.metric.api.ExpectedValue;

import java.util.Map;

import static io.datavines.common.ConfigConstants.METRIC_UNIQUE_KEY;

public class SparkWeeklyAvg implements ExpectedValue {

    @Override
    public String getName() {
        return "weekly_avg";
    }

    @Override
    public String getKey(Map<String,String> inputParameter) {
        String uniqueKey = inputParameter.get(METRIC_UNIQUE_KEY);
        return String.format("weekly_range_%s.expected_value_%s",uniqueKey,uniqueKey);
    }

    @Override
    public String getZhName() {
        return "周均值";
    }

    @Override
    public String getExecuteSql(Map<String,String> inputParameter) {
        String uniqueKey = inputParameter.get(METRIC_UNIQUE_KEY);
        return "select round(avg(actual_value),2) as expected_value_" + uniqueKey +
                " from dv_actual_values where data_time >= date_sub(date_format(${data_time},'yyyy-MM-dd'), (7- datediff(next_day(date_format(${data_time}, 'yyyy-MM-dd'),'Sunday'),date_format(${data_time}, 'yyyy-MM-dd')))-1)" +
                " and data_time < date_add(date_format(${data_time}, 'yyyy-MM-dd'),1) and unique_code = ${unique_code}";
    }

    @Override
    public String getOutputTable(Map<String,String> inputParameter) {
        String uniqueKey = inputParameter.get(METRIC_UNIQUE_KEY);
        return "weekly_range_" + uniqueKey;
    }

    @Override
    public boolean isNeedDefaultDatasource() {
        return true;
    }

    @Override
    public void prepare(Map<String, String> config) {

    }
}
