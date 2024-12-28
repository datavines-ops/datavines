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
package io.datavines.engine.flink.api.stream;

import io.datavines.common.config.CheckResult;
import io.datavines.common.config.Config;
import io.datavines.engine.api.component.Component;
import io.datavines.engine.api.env.Execution;
import io.datavines.engine.api.plugin.Plugin;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink>, Plugin {

    private final FlinkRuntimeEnvironment flinkEnv;
    private Config config;

    public FlinkStreamExecution(FlinkRuntimeEnvironment flinkEnv) {
        this.flinkEnv = flinkEnv;
        this.config = new Config();
    }

    @Override
    public void setConfig(Config config) {
        if (config != null) {
            this.config = config;
        }
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "Configuration check passed");
    }

    public String getType() {
        return "flink_stream";
    }

    @Override
    public void prepare() throws Exception {
        // Initialization if needed
    }

    @Override
    public void execute(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) throws Exception {
        for (FlinkStreamSource source : sources) {
            DataStream<Row> sourceStream = source.getData(flinkEnv);
            createTemporaryView(source.getClass().getSimpleName(), sourceStream, source.getFieldNames());

            DataStream<Row> transformedStream = sourceStream;
            for (FlinkStreamTransform transform : transforms) {
                transformedStream = transform.process(transformedStream, flinkEnv);
                createTemporaryView(transform.getClass().getSimpleName(), transformedStream, transform.getOutputFieldNames());
            }

            for (FlinkStreamSink sink : sinks) {
                sink.output(transformedStream, flinkEnv);
            }
        }

        flinkEnv.getEnv().execute();
    }

    @Override
    public void stop() throws Exception {
        // Flink's execution doesn't need explicit stopping
    }

    private void createTemporaryView(String tableName, DataStream<Row> dataStream, String[] fieldNames) {
        StreamTableEnvironment tableEnv = flinkEnv.getTableEnv();
        Table table = tableEnv.fromDataStream(dataStream);
        for (int i = 0; i < fieldNames.length; i++) {
            table = table.as(fieldNames[i]);
        }
        tableEnv.createTemporaryView(tableName, table);
    }
}
