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

import io.datavines.engine.api.component.Component;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import io.datavines.engine.flink.api.FlinkRuntimeEnvironment;

public interface FlinkStreamTransform extends Component {
    
    /**
     * 处理数据流
     */
    DataStream<Row> process(DataStream<Row> dataStream, FlinkRuntimeEnvironment environment);
    
    /**
     * 获取输出Schema
     */
    String[] getOutputFieldNames();
    
    /**
     * 获取输出数据类型
     */
    Class<?>[] getOutputFieldTypes();
}
