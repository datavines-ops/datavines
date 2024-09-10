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
package io.datavines.notification.plugin.dingtalk;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datavines.common.param.form.ParamsOptions;
import io.datavines.common.param.form.PluginParams;
import io.datavines.common.param.form.Validate;
import io.datavines.common.param.form.type.InputParam;
import io.datavines.common.param.form.type.RadioParam;
import io.datavines.common.utils.JSONUtils;
import io.datavines.notification.api.entity.*;
import io.datavines.notification.api.spi.SlasHandlerPlugin;
import io.datavines.notification.plugin.dingtalk.entity.ReceiverConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DingTalkSlasHandlerPlugin implements SlasHandlerPlugin {

    private final String STRING_YES = "YES";

    private final String STRING_NO = "NO";

    private final String STRING_TRUE = "TRUE";

    private final String STRING_FALSE = "FALSE";

    @Override
    public SlaNotificationResult notify(SlaNotificationMessage slaNotificationMessage, Map<SlaSenderMessage, Set<SlaConfigMessage>> config) {
        Set<SlaSenderMessage> dingTalkSenderSet = config.keySet().stream().filter(x -> "dingtalk".equals(x.getType())).collect(Collectors.toSet());
        SlaNotificationResult result = new SlaNotificationResult();
        ArrayList<SlaNotificationResultRecord> records = new ArrayList<>();
        result.setStatus(true);
        String subject = slaNotificationMessage.getSubject();
        String message = slaNotificationMessage.getMessage();
        for (SlaSenderMessage senderMessage: dingTalkSenderSet) {
            DingTalkSender dingTalkSender = new DingTalkSender(senderMessage);
            Set<SlaConfigMessage> slaConfigMessageSet = config.get(senderMessage);
            HashSet<ReceiverConfig> toReceivers = new HashSet<>();
            for (SlaConfigMessage receiver: slaConfigMessageSet) {
                String receiverConfigStr = receiver.getConfig();
                ReceiverConfig receiverConfig = JSONUtils.parseObject(receiverConfigStr, ReceiverConfig.class);
                toReceivers.add(receiverConfig);
            }

            SlaNotificationResultRecord record = dingTalkSender.sendCardMsg(toReceivers, subject, message);
            if (record.getStatus().equals(false)) {
                record.setMessage(record.getMessage());
                result.setStatus(false);
            }
            records.add(record);
        }
        result.setRecords(records);
        return result;
    }

    @Override
    public String getConfigSenderJson() {

        List<PluginParams> paramsList = new ArrayList<>();

        InputParam webHook = InputParam.newBuilder("webHook", "webHook")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();
        InputParam secret = InputParam.newBuilder("secret", "secret")
                .addValidate(Validate.newBuilder().setRequired(false).build())
                .build();
        InputParam keyWord = InputParam.newBuilder("keyWord", "keyWord")
                .addValidate(Validate.newBuilder().setRequired(false).build())
                .build();

        paramsList.add(webHook);
        paramsList.add(secret);
        paramsList.add(keyWord);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;

        try {
            result = mapper.writeValueAsString(paramsList);
        } catch (JsonProcessingException e) {
            log.error("json parse error : {}", e.getMessage(), e);
        }

        return result;
    }

    @Override
    public String getConfigJson() {

        List<PluginParams> paramsList = new ArrayList<>();

        InputParam atMobiles = InputParam.newBuilder("atMobiles", "atMobiles")
                .addValidate(Validate.newBuilder().setRequired(false).build())
                .build();
        InputParam atDingtalkIds = InputParam.newBuilder("atDingtalkIds", "atDingtalkIds")
                .addValidate(Validate.newBuilder().setRequired(false).build())
                .build();
        RadioParam isAtAll = RadioParam.newBuilder("isAtAll", "isAtAll")
                .addParamsOptions(new ParamsOptions(STRING_YES, STRING_TRUE, false))
                .addParamsOptions(new ParamsOptions(STRING_NO, STRING_FALSE, false))
                .setValue(STRING_FALSE)
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();

        paramsList.add(atMobiles);
        paramsList.add(atDingtalkIds);
        paramsList.add(isAtAll);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;

        try {
            result = mapper.writeValueAsString(paramsList);
        } catch (JsonProcessingException e) {
            log.error("json parse error : {}", e.getMessage(), e);
        }

        return result;
    }
}
