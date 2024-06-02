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
package io.datavines.metric.api;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public enum MetricDirectionType {
    /**
     * 0-single_table
     * 1-single_table_custom_sql
     * 2-multi_table_accuracy
     * 3-multi_table_comparison
     */
    POSITIVE(0,"positive"),
    NEGATIVE(1,"negative");

    MetricDirectionType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    @Getter
    private final int code;
    private final String description;

    @JsonValue
    public String getDescription() {
        return description;
    }

    private static final Map<Integer, MetricDirectionType> VALUES_MAP = new HashMap<>();

    static {
        for (MetricDirectionType type : MetricDirectionType.values()) {
            VALUES_MAP.put(type.code,type);
        }
    }

    public static MetricDirectionType of(Integer code) {
        if (VALUES_MAP.containsKey(code)) {
            return VALUES_MAP.get(code);
        }
        throw new IllegalArgumentException("invalid code : " + code);
    }

}