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

import io.datavines.common.config.Config;
import io.datavines.common.config.EnvConfig;
import io.datavines.common.config.SinkConfig;
import io.datavines.common.config.SourceConfig;
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

import static io.datavines.common.CommonConstants.*;
import static io.datavines.common.ConfigConstants.*;
import static io.datavines.common.ConfigConstants.TABLE;

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
                
                metricInputParameter.put(METRIC_UNIQUE_KEY, metricUniqueKey);
                String expectedType = jobExecutionInfo.getEngineType() + "_" + parameter.getExpectedType();
                ExpectedValue expectedValue = PluginLoader
                        .getPluginLoader(ExpectedValue.class)
                        .getNewPlugin(expectedType);

                metricInputParameter.put(UNIQUE_CODE, StringUtils.wrapperSingleQuotes(MetricParserUtils.generateUniqueCode(metricInputParameter)));

                // Get the actual value storage parameter
                SinkConfig actualValueSinkConfig = getValidateResultDataSinkConfig(
                        expectedValue, FlinkSinkSqlBuilder.getActualValueSql(), "dv_actual_values", metricInputParameter);
                if (actualValueSinkConfig != null) {
                    sinkConfigs.add(actualValueSinkConfig);
                }

                String taskSinkSql = FlinkSinkSqlBuilder.getDefaultSinkSql();
                if (StringUtils.isEmpty(expectedValue.getOutputTable(metricInputParameter))) {
                    taskSinkSql = taskSinkSql.replaceAll("full join \\$\\{expected_table}", "");
                }

                // Get the task data storage parameter
                SinkConfig taskResultSinkConfig = getValidateResultDataSinkConfig(
                        expectedValue, taskSinkSql, "dv_job_execution_result", metricInputParameter);
                if (taskResultSinkConfig != null) {
                    sinkConfigs.add(taskResultSinkConfig);
                }

                // Get the error data storage parameter
                // Support file(hdfs/minio/s3)/es
                SinkConfig errorDataSinkConfig = getErrorSinkConfig(metricInputParameter);
                if (errorDataSinkConfig != null) {
                    sinkConfigs.add(errorDataSinkConfig);
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
