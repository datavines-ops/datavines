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
package io.datavines.connector.plugin;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datavines.common.CommonConstants;
import io.datavines.common.param.form.PluginParams;
import io.datavines.common.param.form.PropsType;
import io.datavines.common.param.form.Validate;
import io.datavines.common.param.form.props.InputParamsProps;
import io.datavines.common.param.form.type.InputParam;
import io.datavines.connector.api.ConfigBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class JdbcConfigBuilder implements ConfigBuilder {

    @Override
    public String build(boolean isEn) {
        List<PluginParams> params = new ArrayList<>();
        params.add(getHostInput(isEn));
        params.add(getPortInput(isEn));
        if (getCatalogInput(isEn) != null) {
            params.add(getCatalogInput(isEn));
        }

        params.add(getDatabaseInput(isEn));

        if (getSchemaInput(isEn) != null) {
            params.add(getSchemaInput(isEn));
        }

        params.add(getUserInput(isEn));
        params.add(getPasswordInput(isEn));
        params.add(getPropertiesInput(isEn));

        params.addAll(getOtherParams(isEn));

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;

        try {
            result = mapper.writeValueAsString(params);
        } catch (JsonProcessingException e) {
            log.error("json parse error : ", e);
        }

        return result;
    }

    @Override
    public String buildErrorDataStorage(boolean isEn) {
        return this.build(isEn);
    }

    protected InputParam getHostInput(boolean isEn) {
        return getInputParam("host",
                isEn ? "host":"地址",
                isEn ? "please enter host ip" : "请填入连接地址", 1,
                Validate.newBuilder().setRequired(true).setMessage(isEn ? "please enter host ip" : "请填入连接地址").build(),
                null);
    }

    protected InputParam getPortInput(boolean isEn) {
        return getInputParam("port",
                isEn ? "port" : "端口",
                isEn ? "please enter port" : "请填入端口号", 1,
                Validate.newBuilder().setRequired(true).setMessage(isEn ? "please enter port" : "请填入端口号").build(),
                null);
    }

    protected InputParam getCatalogInput(boolean isEn) {
        return null;
    }

    protected InputParam getSchemaInput(boolean isEn) {
        return null;
    }

    protected InputParam getDatabaseInput(boolean isEn) {
        return getInputParam("database",
                isEn ? "database" : "数据库",
                isEn ? "please enter database" : "请填入数据库", 1, Validate.newBuilder().setRequired(true).setMessage(isEn ? "please enter database" : "请填入数据库").build(),
                null);
    }

    protected InputParam getUserInput(boolean isEn) {
        return getInputParam("user",
                isEn ? "user" : "用户名",
                isEn ? "please enter user" : "请填入用户名", 1,
                Validate.newBuilder().setRequired(true).setMessage(isEn ? "please enter user" : "请填入用户名").build(),
                null);
    }

    protected InputParam getPasswordInput(boolean isEn) {
        return getInputParam("password",
                isEn ? "password" : "密码",
                isEn ? "please enter password" : "请填入密码", 1,
                Validate.newBuilder().setRequired(true).setMessage(isEn ? "please enter password" : "请填入密码").build(),
                null);
    }

    protected InputParam getPropertiesInput(boolean isEn) {
        return getInputParam("properties",
                isEn ? "properties" : "参数",
                isEn ? "please enter properties,like key=value&key1=value1" : "请填入参数，格式为key=value&key1=value1", 2, null,
                null);
    }

    protected InputParam getInputParam(String field, String title, String placeholder, int rows, Validate validate , Object defaultValue) {
        return InputParam
                .newBuilder(field, title)
                .addValidate(validate)
                .setProps(new InputParamsProps().setDisabled(false))
                .setSize(CommonConstants.SMALL)
                .setType(PropsType.TEXT)
                .setRows(rows)
                .setPlaceholder(placeholder)
                .setValue(defaultValue)
                .setEmit(null)
                .build();
    }

    protected List<PluginParams> getOtherParams(boolean isEn) {

        List<PluginParams> list = new ArrayList<>();

        InputParam enableExternalCatalog = getInputParam("enable_use_view",
                isEn ? "enable.use.view" : "允许使用视图",
                isEn ? "please enter true or false" : "请填入 true 或者 false", 2, null,
                "false");

        list.add(enableExternalCatalog);
        return list;
    }

}
