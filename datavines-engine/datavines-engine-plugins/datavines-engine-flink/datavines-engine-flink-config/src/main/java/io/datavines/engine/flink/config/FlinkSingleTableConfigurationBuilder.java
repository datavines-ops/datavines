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

import io.datavines.common.config.EnvConfig;
import io.datavines.common.config.SinkConfig;
import io.datavines.common.config.SourceConfig;
import io.datavines.common.config.enums.SinkType;
import io.datavines.common.entity.job.BaseJobParameter;
import io.datavines.common.exception.DataVinesException;
import io.datavines.common.utils.StringUtils;
import io.datavines.engine.config.MetricParserUtils;
import io.datavines.metric.api.ExpectedValue;
import io.datavines.spi.PluginLoader;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.datavines.common.ConfigConstants.*;

public class FlinkSingleTableConfigurationBuilder extends BaseFlinkConfigurationBuilder {


    @Override
    public void buildEnvConfig() {
        EnvConfig envConfig = new EnvConfig();
        envConfig.setEngine("flink");
        configuration.setEnvConfig(envConfig);
    }

    @Override
    public void buildSinkConfigs() throws DataVinesException {
        List<SinkConfig> sinkConfigs = new ArrayList<>();

        List<BaseJobParameter> metricJobParameterList = jobExecutionParameter.getMetricParameterList();
        if (CollectionUtils.isNotEmpty(metricJobParameterList)) {
            for (BaseJobParameter parameter : metricJobParameterList) {
                String metricUniqueKey = getMetricUniqueKey(parameter);
                Map<String, String> metricInputParameter = metric2InputParameter.get(metricUniqueKey);
                if (metricInputParameter == null) {
                    continue;
                }
                
                // 确保必要的参数存在
                if (!metricInputParameter.containsKey(METRIC_NAME) && parameter.getMetricType() != null) {
                    metricInputParameter.put(METRIC_NAME, parameter.getMetricType());
                }
                
                metricInputParameter.put(METRIC_UNIQUE_KEY, metricUniqueKey);
                String expectedType = "local_" + parameter.getExpectedType();
                ExpectedValue expectedValue = PluginLoader
                        .getPluginLoader(ExpectedValue.class)
                        .getNewPlugin(expectedType);

                // 只有在确保必要参数存在的情况下才生成 uniqueCode
                if (metricInputParameter.containsKey(METRIC_NAME)) {
                    metricInputParameter.put(UNIQUE_CODE, StringUtils.wrapperSingleQuotes(MetricParserUtils.generateUniqueCode(metricInputParameter)));
                }

                // Get the actual value storage parameter
                String actualValueSinkSql = FlinkSinkSqlBuilder.getActualValueSql()
                        .replace("${actual_value}", "${actual_value_" + metricUniqueKey + "}");
                SinkConfig actualValueSinkConfig = getValidateResultDataSinkConfig(
                        expectedValue, actualValueSinkSql, "dv_actual_values", metricInputParameter);
                
                if (actualValueSinkConfig != null) {
                    actualValueSinkConfig.setType(SinkType.ACTUAL_VALUE.getDescription());
                    sinkConfigs.add(actualValueSinkConfig);
                }

                String taskSinkSql = FlinkSinkSqlBuilder.getDefaultSinkSql()
                        .replace("${actual_value}", "${actual_value_" + metricUniqueKey + "}")
                        .replace("${expected_value}", "${expected_value_" + metricUniqueKey + "}");
                
                // Get the task data storage parameter
                SinkConfig taskResultSinkConfig = getValidateResultDataSinkConfig(
                        expectedValue, taskSinkSql, "dv_job_execution_result", metricInputParameter);
                if (taskResultSinkConfig != null) {
                    taskResultSinkConfig.setType(SinkType.VALIDATE_RESULT.getDescription());
                    // 设置默认状态为未知（NONE）
                    taskResultSinkConfig.getConfig().put("default_state", "0");
                    // 添加其他必要参数
                    taskResultSinkConfig.getConfig().put("metric_type", "single_table");
                    taskResultSinkConfig.getConfig().put("metric_name", metricInputParameter.get(METRIC_NAME));
                    taskResultSinkConfig.getConfig().put("metric_dimension", metricInputParameter.get(METRIC_DIMENSION));
                    taskResultSinkConfig.getConfig().put("database_name", metricInputParameter.get(DATABASE));
                    taskResultSinkConfig.getConfig().put("table_name", metricInputParameter.get(TABLE));
                    taskResultSinkConfig.getConfig().put("column_name", metricInputParameter.get(COLUMN));
                    taskResultSinkConfig.getConfig().put("expected_type", metricInputParameter.get(EXPECTED_TYPE));
                    taskResultSinkConfig.getConfig().put("result_formula", metricInputParameter.get(RESULT_FORMULA));
                    sinkConfigs.add(taskResultSinkConfig);
                }

                // Get the error data storage parameter if needed
                if (StringUtils.isNotEmpty(jobExecutionInfo.getErrorDataStorageType())
                        && StringUtils.isNotEmpty(jobExecutionInfo.getErrorDataStorageParameter())) {
                    SinkConfig errorDataSinkConfig = getErrorSinkConfig(metricInputParameter);
                    if (errorDataSinkConfig != null) {
                        errorDataSinkConfig.setType(SinkType.ERROR_DATA.getDescription());
                        sinkConfigs.add(errorDataSinkConfig);
                    }
                }
            }
        }

        configuration.setSinkParameters(sinkConfigs);
    }

    @Override
    public void buildTransformConfigs() {
        // No transform configs needed for single table configuration
    }

    @Override
    public void buildSourceConfigs() throws DataVinesException {
        List<SourceConfig> sourceConfigs = getSourceConfigs();
        configuration.setSourceParameters(sourceConfigs);
    }

    @Override
    public void buildName() {
        // Use default name from base implementation
    }
}
