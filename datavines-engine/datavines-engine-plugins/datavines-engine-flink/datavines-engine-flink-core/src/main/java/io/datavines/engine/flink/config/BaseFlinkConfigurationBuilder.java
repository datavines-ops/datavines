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
import io.datavines.common.config.enums.SourceType;
import io.datavines.common.entity.ConnectorParameter;
import io.datavines.common.entity.job.BaseJobParameter;
import io.datavines.common.exception.DataVinesException;
import io.datavines.common.utils.JSONUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.engine.common.utils.ParserUtils;
import io.datavines.engine.config.BaseJobConfigurationBuilder;
import io.datavines.connector.api.ConnectorFactory;
import io.datavines.spi.PluginLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static io.datavines.common.CommonConstants.*;
import static io.datavines.common.ConfigConstants.*;
import static io.datavines.common.ConfigConstants.TABLE;

/**
 * 
 *
 * @author dataVines
 * @since 2021-07-01
 */
@Slf4j
public abstract class BaseFlinkConfigurationBuilder extends BaseJobConfigurationBuilder {

    @Override
    protected EnvConfig getEnvConfig() {
        EnvConfig envConfig = new EnvConfig();
        envConfig.setEngine(jobExecutionInfo.getEngineType());
        Map<String,Object> configMap = envConfig.getConfig();
        if (configMap == null) {
            configMap = new HashMap<>();
        }

        ConnectorParameter connectorParameter = jobExecutionParameter.getConnectorParameter();
        String srcConnectorType = "";
        if (connectorParameter != null) {
            srcConnectorType = connectorParameter.getType();
        }

        ConnectorParameter connectorParameter2 = jobExecutionParameter.getConnectorParameter2();
        String srcConnectorType2 = "";
        if (connectorParameter2 != null) {
            srcConnectorType2 = connectorParameter2.getType();
        }

        envConfig.setConfig(configMap);
        return envConfig;
    }

    @Override
    protected List<SourceConfig> getSourceConfigs() throws DataVinesException {
        List<SourceConfig> sourceConfigs = new ArrayList<>();
        List<BaseJobParameter> metricJobParameterList = jobExecutionParameter.getMetricParameterList();
        boolean isAddValidateResultDataSource = false;
        if (CollectionUtils.isNotEmpty(metricJobParameterList)) {
            Set<String> sourceConnectorSet = new HashSet<>();
            Set<String> targetConnectorSet = new HashSet<>();
            for (BaseJobParameter parameter : metricJobParameterList) {
                String metricUniqueKey = getMetricUniqueKey(parameter);
                Map<String, String> metricInputParameter = metric2InputParameter.get(metricUniqueKey);
                if (jobExecutionParameter.getConnectorParameter() != null) {
                    ConnectorParameter connectorParameter = jobExecutionParameter.getConnectorParameter();
                    SourceConfig sourceConfig = new SourceConfig();

                    Map<String, Object> connectorParameterMap = new HashMap<>(connectorParameter.getParameters());
                    connectorParameterMap.putAll(metricInputParameter);

                    if (connectorParameter.getParameters().get(SCHEMA) != null) {
                        metricInputParameter.put(SCHEMA, (String)connectorParameter.getParameters().get(SCHEMA));
                    }

                    metricInputParameter.put(DATABASE_NAME, metricInputParameter.get(DATABASE));
                    metricInputParameter.put(TABLE_NAME, metricInputParameter.get(TABLE));
                    metricInputParameter.put(COLUMN_NAME, metricInputParameter.get(COLUMN));

                    ConnectorFactory connectorFactory = PluginLoader
                            .getPluginLoader(ConnectorFactory.class)
                            .getNewPlugin(connectorParameter.getType());

                    connectorParameterMap.put(TABLE, metricInputParameter.get(TABLE));
                    connectorParameterMap.put(DATABASE, metricInputParameter.get(DATABASE));
                    connectorParameterMap = connectorFactory.getConnectorParameterConverter().converter(connectorParameterMap);
                    connectorParameterMap.put(PASSWORD, ParserUtils.encode((String)connectorParameterMap.get(PASSWORD)));

                    String outputTable = getOutputTable(metricInputParameter.get(DATABASE), metricInputParameter.get(SCHEMA), metricInputParameter.get(TABLE));
                    String tableAlias = getTableAlias(metricInputParameter.get(DATABASE), metricInputParameter.get(SCHEMA), metricInputParameter.get(TABLE), "1");
                    connectorParameterMap.put(OUTPUT_TABLE, outputTable);
                    connectorParameterMap.put(DRIVER, connectorFactory.getDialect().getDriver());

                    metricInputParameter.put(TABLE, outputTable);
                    metricInputParameter.put(TABLE_ALIAS, tableAlias);
                    metricInputParameter.put(COLUMN, metricInputParameter.get(COLUMN));
                    metricInputParameter.put(REGEX_KEY, "REGEXP(${column}, ${regex})");
                    metricInputParameter.put(NOT_REGEX_KEY, "NOT REGEXP(${column}, ${regex})");
                    metricInputParameter.put(STRING_TYPE, "STRING");
                    metricInputParameter.put(IF_FUNCTION_KEY, "IF");
                    metricInputParameter.put(LIMIT_TOP_50_KEY, " LIMIT 50");
                    metricInputParameter.put(LENGTH_KEY, "CHARACTER_LENGTH(${column})");
                    metricInputParameter.put(SRC_CONNECTOR_TYPE, connectorParameter.getType());
                    metricInputParameter.put(ENGINE_TYPE, jobExecutionInfo.getEngineType());

                    String connectorUUID = connectorFactory.getConnectorParameterConverter().getConnectorUUID(connectorParameterMap);

                    if (sourceConnectorSet.contains(connectorUUID)) {
                        continue;
                    }

                    sourceConfig.setPlugin(connectorFactory.getCategory());
                    sourceConfig.setConfig(connectorParameterMap);
                    sourceConfig.setType(SourceType.SOURCE.getDescription());
                    sourceConfigs.add(sourceConfig);
                    sourceConnectorSet.add(connectorUUID);
                }

                if (jobExecutionParameter.getConnectorParameter2() != null
                        && jobExecutionParameter.getConnectorParameter2().getParameters() != null) {
                    ConnectorParameter connectorParameter2 = jobExecutionParameter.getConnectorParameter2();
                    SourceConfig sourceConfig = new SourceConfig();

                    Map<String, Object> connectorParameterMap = new HashMap<>(connectorParameter2.getParameters());
                    connectorParameterMap.putAll(metricInputParameter);

                    if (connectorParameter2.getParameters().get(SCHEMA) != null) {
                        metricInputParameter.put(SCHEMA2, (String)connectorParameter2.getParameters().get(SCHEMA));
                    }

                    ConnectorFactory connectorFactory = PluginLoader
                            .getPluginLoader(ConnectorFactory.class)
                            .getNewPlugin(connectorParameter2.getType());

                    connectorParameterMap.put(TABLE, metricInputParameter.get(TABLE2));
                    connectorParameterMap.put(DATABASE, metricInputParameter.get(DATABASE2));
                    connectorParameterMap = connectorFactory.getConnectorParameterConverter().converter(connectorParameterMap);
                    connectorParameterMap.put(PASSWORD, ParserUtils.encode((String)connectorParameterMap.get(PASSWORD)));

                    String outputTable = getOutputTable(metricInputParameter.get(DATABASE2),
                            metricInputParameter.get(SCHEMA2),
                            metricInputParameter.get(TABLE2)) + "_2";

                    String tableAlias = getTableAlias(metricInputParameter.get(DATABASE2),
                            metricInputParameter.get(SCHEMA2),
                            metricInputParameter.get(TABLE2), "2");

                    connectorParameterMap.put(OUTPUT_TABLE, outputTable);
                    connectorParameterMap.put(DRIVER, connectorFactory.getDialect().getDriver());

                    metricInputParameter.put(TABLE2, outputTable);
                    metricInputParameter.put(TABLE2_ALIAS, tableAlias);

                    String connectorUUID = connectorFactory.getConnectorParameterConverter().getConnectorUUID(connectorParameterMap);

                    if (targetConnectorSet.contains(connectorUUID)) {
                        continue;
                    }

                    sourceConfig.setPlugin(connectorFactory.getCategory());
                    sourceConfig.setConfig(connectorParameterMap);
                    sourceConfig.setType(SourceType.SOURCE.getDescription());
                    sourceConfigs.add(sourceConfig);
                    targetConnectorSet.add(connectorUUID);
                }

                metric2InputParameter.put(metricUniqueKey, metricInputParameter);
            }
        }

        return sourceConfigs;
    }

    protected String getOutputTable(String database, String schema, String table) {
        if (StringUtils.isNotEmpty(schema)) {
            return String.format("%s_%s_%s", database, schema, table);
        }
        return String.format("%s_%s", database, table);
    }

    protected String getTableAlias(String database, String schema, String table, String order) {
        if (StringUtils.isNotEmpty(schema)) {
            return String.format("t%s_%s_%s_%s", order, database, schema, table);
        }
        return String.format("t%s_%s_%s", order, database, table);
    }

    protected SinkConfig getErrorSinkConfig(Map<String, String> inputParameter) {
        if (FILE.equalsIgnoreCase(jobExecutionInfo.getErrorDataStorageType())) {
            SinkConfig sinkConfig = new SinkConfig();
            Map<String, Object> configMap = new HashMap<>();
            Map<String,String> errorDataParameterMap = JSONUtils.toMap(jobExecutionInfo.getErrorDataStorageParameter(),String.class, String.class);
            configMap.put(DATA_DIR, errorDataParameterMap.get(DATA_DIR));
            configMap.put(FILE_NAME, inputParameter.get(ERROR_DATA_FILE_NAME));
            configMap.put(COLUMN_SEPARATOR, errorDataParameterMap.get(COLUMN_SEPARATOR));
            configMap.put(LINE_SEPARATOR, errorDataParameterMap.get(LINE_SEPARATOR));
            sinkConfig.setConfig(configMap);
            sinkConfig.setType(SinkType.ERROR_DATA.getDescription());
            sinkConfig.setPlugin(FILE);
            return sinkConfig;
        }
        return null;
    }
}
