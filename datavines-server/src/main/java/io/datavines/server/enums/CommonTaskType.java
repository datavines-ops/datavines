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
package io.datavines.server.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

import java.util.HashMap;
import java.util.Map;

public enum CommonTaskType {

    /**
     * 0 - catalog metadata fetch
     * 1 - data quality report
     */
    CATALOG_METADATA_FETCH(0, "catalog_metadata_fetch", "元数据抓取"),
    DATA_QUALITY_REPORT(1, "data_quality_report", "数据质量报告")
    ;

    CommonTaskType(int code, String description, String zhDescription){
        this.code = code;
        this.description = description;
        this.zhDescription = zhDescription;
    }

    private final int code;

    @EnumValue
    private final String description;

    private final String zhDescription;

    private static final Map<Integer, CommonTaskType> FETCH_TYPE_MAP = new HashMap<>();

    static {
        for (CommonTaskType fetchType : CommonTaskType.values()) {
            FETCH_TYPE_MAP.put(fetchType.code,fetchType);
        }
    }

    public static CommonTaskType of(Integer status) {
        if (FETCH_TYPE_MAP.containsKey(status)) {
            return FETCH_TYPE_MAP.get(status);
        }
        throw new IllegalArgumentException("invalid fetch type : " + status);
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public String getZhDescription() {
        return zhDescription;
    }
}
