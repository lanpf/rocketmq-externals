package org.apache.rocketmq.spring.starter.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.starter.core.RocketMQMessageConst;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.util.Objects;

/**
 * @author lanpengfei
 * @date 2018/10/14
 */
public class RocketMQMessageUtil {

    public static org.apache.rocketmq.common.message.Message convertToRocketMsg(String destination, Message<?> message, ObjectMapper objectMapper, String charset) {
        Object payloadObj = message.getPayload();
        byte[] payloads;

        if (payloadObj instanceof String) {
            payloads = ((String) payloadObj).getBytes(Charset.forName(charset));
        } else {
            try {
                String jsonObj = objectMapper.writeValueAsString(payloadObj);
                payloads = jsonObj.getBytes(Charset.forName(charset));
            } catch (Exception e) {
                throw new RuntimeException("convert to RocketMQ message failed.", e);
            }
        }

        String[] split = destination.split(":", 2);
        String topic = split[0];
        String tags = split.length > 1 ? split[1] : "";


        org.apache.rocketmq.common.message.Message rocketMsg = new org.apache.rocketmq.common.message.Message(topic, tags, payloads);

        MessageHeaders headers = message.getHeaders();
        if (Objects.nonNull(headers) && !headers.isEmpty()) {
            Object keys = headers.get(MessageConst.PROPERTY_KEYS);
            // if headers has 'KEYS', set rocketMQ message key
            if (!StringUtils.isEmpty(keys)) {
                rocketMsg.setKeys(keys.toString());
            }

            // set rocketMQ message flag
            Object flagObj = headers.getOrDefault(RocketMQMessageConst.CUSTOM_PROPERTY_FLAG, "0");
            int flag = 0;
            try {
                flag = Integer.parseInt(flagObj.toString());
            } catch (NumberFormatException e) {
                // ignore
            }
            rocketMsg.setFlag(flag);

            // set rocketMQ message waitStoreMsgOkObj
            Object waitStoreMsgOkObj = headers.getOrDefault(RocketMQMessageConst.CUSTOM_PROPERTY_WAIT_STORE_MSG_OK, true);
            boolean waitStoreMsgOK = Boolean.TRUE.equals(waitStoreMsgOkObj);
            rocketMsg.setWaitStoreMsgOK(waitStoreMsgOK);

            // exclude "KEYS", "FLAG", "WAIT_STORE_MSG_OK"
            headers.entrySet().stream()
                    .filter(entry -> !Objects.equals(entry.getKey(), MessageConst.PROPERTY_KEYS)
                            && !Objects.equals(entry.getKey(), RocketMQMessageConst.CUSTOM_PROPERTY_FLAG)
                            && !Objects.equals(entry.getKey(), RocketMQMessageConst.CUSTOM_PROPERTY_WAIT_STORE_MSG_OK))
                    .forEach(entry -> rocketMsg.putUserProperty(RocketMQMessageConst.CUSTOM + "_" + entry.getKey(), String.valueOf(entry.getValue())));

        }

        return rocketMsg;
    }
}
