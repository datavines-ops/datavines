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

import io.datavines.common.utils.JSONUtils;
import io.datavines.notification.api.entity.SlaNotificationResultRecord;
import io.datavines.notification.api.entity.SlaSenderMessage;
import io.datavines.notification.plugin.dingtalk.entity.ReceiverConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Slf4j
@EqualsAndHashCode
@Data
public class DingTalkSender {

    private String msgType;
    private String webHook;
    private String secret;
    private String keyWord;

    private String mustNotNull = " must not be null";

    public DingTalkSender(SlaSenderMessage senderMessage) {

        String configString = senderMessage.getConfig();
        Map<String, String> config = JSONUtils.toMap(configString);

        msgType=config.get("msgType");

        webHook=config.get("webHook");
        requireNonNull(webHook, "dingtalk webHook" + mustNotNull);
        secret=config.get("secret");
        keyWord=config.get("keyWord");

    }

    public SlaNotificationResultRecord sendCardMsg(Set<ReceiverConfig> receiverSet, String subject, String message){
        SlaNotificationResultRecord result = new SlaNotificationResultRecord();
        if (CollectionUtils.isEmpty(receiverSet)) {
            return result;
        }
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        Set<ReceiverConfig> failToReceivers = new HashSet<>();
        for(ReceiverConfig receiverConfig : receiverSet){
            try {
                String msg = generateMsgJson(subject, message, receiverConfig);
                String url = constructUrl();
                HttpPost httpPost = constructHttpPost(url, msg);
                CloseableHttpClient httpClient = getDefaultClient();
                try {
                    CloseableHttpResponse response = httpClient.execute(httpPost);
                    String resp;
                    try {
                        HttpEntity entity = response.getEntity();
                        resp = EntityUtils.toString(entity, "UTF-8");
                        EntityUtils.consume(entity);
                    } finally {
                        response.close();
                    }
                    log.info("Ding Talk send msg :{}, resp: {}", msg, resp);
                } finally {
                    httpClient.close();
                }
            } catch (Exception e) {
                failToReceivers.add(receiverConfig);
                log.error("dingtalk send error", e);
            }
        }

        if (!CollectionUtils.isEmpty(failToReceivers)) {
            String recordMessage = String.format("send to %s fail", String.join(",", failToReceivers.stream().map(ReceiverConfig::getAtMobiles).collect(Collectors.toList())));
            result.setStatus(false);
            result.setMessage(recordMessage);
        }else{
            result.setStatus(true);
        }
        return result;
    }

    private String constructUrl() throws NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {
        if (org.apache.commons.lang3.StringUtils.isBlank(secret)) {
            return webHook;
        }
        Long timestamp = System.currentTimeMillis();
        String stringToSign = timestamp + "\n" + secret;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"));
        byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
        String sign = URLEncoder.encode(new String(Base64.encodeBase64(signData)),"UTF-8");

        // sign字段和timestamp字段必须拼接到请求URL上，否则会出现 310000 的错误信息
        return String.format(webHook + "&sign=%s&timestamp=%d", sign, timestamp);
    }

    private String generateMsgJson(String title, String content,ReceiverConfig receiverConfig) {

        final String atMobiles = receiverConfig.getAtMobiles();
        final String atDingtalkIds = receiverConfig.getAtDingtalkIds();
        final Boolean isAtAll = receiverConfig.getIsAtAll();

        if (org.apache.commons.lang3.StringUtils.isBlank(msgType)) {
            msgType = DingTalkConstants.DING_TALK_MSG_TYPE_TEXT;
        }
        Map<String, Object> items = new HashMap<>();
        items.put("msgtype", msgType);
        Map<String, Object> text = new HashMap<>();
        items.put(msgType, text);

        if (DingTalkConstants.DING_TALK_MSG_TYPE_MARKDOWN.equals(msgType)) {
            StringBuilder builder = new StringBuilder(content);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(keyWord)) {
                builder.append(" ");
                builder.append(keyWord);
            }
            builder.append("\n\n");
            if (org.apache.commons.lang3.StringUtils.isNotBlank(atMobiles)) {
                Arrays.stream(atMobiles.split(",")).forEach(value -> {
                    builder.append("@");
                    builder.append(value);
                    builder.append(" ");
                });
            }
            if (org.apache.commons.lang3.StringUtils.isNotBlank(atDingtalkIds)) {
                Arrays.stream(atDingtalkIds.split(",")).forEach(value -> {
                    builder.append("@");
                    builder.append(value);
                    builder.append(" ");
                });
            }
            byte[] byt = StringUtils.getBytesUtf8(builder.toString());
            String txt = StringUtils.newStringUtf8(byt);
            text.put("title", title);
            text.put("text", txt);
        } else {
            StringBuilder builder = new StringBuilder(title);
            builder.append("\n");
            builder.append(content);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(keyWord)) {
                builder.append(" ");
                builder.append(keyWord);
            }
            byte[] byt = StringUtils.getBytesUtf8(builder.toString());
            String txt = StringUtils.newStringUtf8(byt);
            text.put("content", txt);
        }

        Map<String, Object> at = new HashMap<>();

        String[] atMobileArray =
                org.apache.commons.lang3.StringUtils.isNotBlank(atMobiles) ? atMobiles.split(",")
                        : new String[0];
        String[] atUserArray =
                org.apache.commons.lang3.StringUtils.isNotBlank(atDingtalkIds) ? atDingtalkIds.split(",")
                        : new String[0];
        at.put("atMobiles", atMobileArray);
        at.put("atDingtalkIds", atUserArray);
        at.put("isAtAll", isAtAll);

        items.put("at", at);

        return JSONUtils.toJsonString(items);

    }
    private static HttpPost constructHttpPost(String url, String msg) {
        HttpPost post = new HttpPost(url);
        StringEntity entity = new StringEntity(msg, StandardCharsets.UTF_8);
        post.setEntity(entity);
        post.addHeader("Content-Type", "application/json; charset=utf-8");
        return post;
    }

    private static CloseableHttpClient getDefaultClient() {
        return HttpClients.createDefault();
    }

}
