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
package io.datavines.notification.plugin.wecombot;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.datavines.common.utils.HttpUtils;
import io.datavines.common.utils.JSONUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.notification.api.entity.SlaNotificationResultRecord;
import io.datavines.notification.api.entity.SlaSenderMessage;
import io.datavines.notification.plugin.wecombot.entity.WecomBotRes;
import io.datavines.notification.plugin.wecombot.utils.ContentUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static java.util.Objects.requireNonNull;

@Slf4j
@EqualsAndHashCode
@Data
public class WecomBotSender {

    private String webhookList;

    public WecomBotSender(SlaSenderMessage senderMessage) {
        String configString = senderMessage.getConfig();
        Map<String, String> config = JSONUtils.toMap(configString);
        webhookList = config.get("webhook");
        requireNonNull(webhookList, "webhook must not be null");

    }

    public SlaNotificationResultRecord sendMsg(Set<String> receiverSet, String subject, String message) {
        SlaNotificationResultRecord result = new SlaNotificationResultRecord();
        // if there is no receivers && no receiversCc, no need to process
        if (CollectionUtils.isEmpty(receiverSet)) {
            return result;
        }
        receiverSet.removeIf(StringUtils::isEmpty);
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        // send msg
        Set<String> failToReceivers = new HashSet<>();
        for (String receiver : receiverSet) {
            try {
                String markdownMessage = getMarkdownMessage(subject, message);
                Map<String, Object> paramMap = ContentUtil.createParamMap(WecomBotConstants.MSG_TYPE, WecomBotConstants.MARKDOWN, WecomBotConstants.MARKDOWN, ContentUtil.createParamMap(WecomBotConstants.CONTENT, markdownMessage));
                String res = HttpUtils.post(receiver, JSONUtils.toJsonString(paramMap), null);
                WecomBotRes wecomBotRes = WecomBotRes.parseFromJson(res);
                if(!wecomBotRes.success()){
                    failToReceivers.add(receiver);
                    log.info("wecomBot sender error, please check config! webhook: {} , param: {}, res: {}", receiver, JSONUtils.toJsonString(paramMap), res);
                }
            } catch (Exception e) {
                failToReceivers.add(receiver);
                log.error("wecomBot send error", e);
            }
        }
        if (!CollectionUtils.isEmpty(failToReceivers)) {
            String recordMessage = String.format("send to %s fail", String.join(",", failToReceivers));
            result.setStatus(false);
            result.setMessage(recordMessage);
        } else {
            result.setStatus(true);
        }
        return result;
    }

    private String getMarkdownMessage(String subject, String content) {
        StringBuilder contents = new StringBuilder(100);
        if (StringUtils.isNotEmpty(subject)) {
            contents.append(WecomBotConstants.FIRST_TITLE_START).append(subject).append(WecomBotConstants.END);
        }
        if (StringUtils.isNotEmpty(content)) {
            ArrayNode list = JSONUtils.parseArray(content);
            for (JsonNode jsonNode : list) {
                String nodeMessage = jsonNode.toString().replace("\"", "");
                if (nodeMessage.startsWith("Task Execution Record")||nodeMessage.startsWith("任务执行记录")){
                    String formatMessage = String.format("%s : [%s](%s)", nodeMessage.substring(0,nodeMessage.indexOf(" : ")),nodeMessage.substring(nodeMessage.indexOf(":")+2),nodeMessage.substring(nodeMessage.indexOf(":")+2));
                    contents.append(WecomBotConstants.QUOTE_START).append(formatMessage).append(WecomBotConstants.END);
                }else {
                    contents.append(WecomBotConstants.QUOTE_START).append(nodeMessage).append(WecomBotConstants.END);
                }
            }
        }
        return contents.toString();
    }
}
