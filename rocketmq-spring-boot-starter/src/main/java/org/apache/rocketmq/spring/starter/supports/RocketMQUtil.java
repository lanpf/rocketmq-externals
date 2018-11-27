package org.apache.rocketmq.spring.starter.supports;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.starter.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.starter.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.util.Objects;

/**
 * @author lanpengfei
 * @date 2018/10/14
 */
@Slf4j
public class RocketMQUtil {

    public static TransactionListener convertToTransactionListener(RocketMQLocalTransactionListener listener) {
        return new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(org.apache.rocketmq.common.message.Message message, Object obj) {
                RocketMQLocalTransactionState state = listener.executeLocalTransaction(convertToSpringMassage(message), obj);
                return convertToLocalTransactionState(state);
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                RocketMQLocalTransactionState state = listener.checkLocalTransaction(convertToSpringMassage(messageExt));
                return convertToLocalTransactionState(state);
            }
        };
    }

    private static LocalTransactionState convertToLocalTransactionState(RocketMQLocalTransactionState state) {
        switch (state) {
            case UNKNOW:
                return LocalTransactionState.UNKNOW;
            case COMMIT_MESSAGE:
                return LocalTransactionState.COMMIT_MESSAGE;
            case ROLLBACK_MESSAGE:
                return LocalTransactionState.ROLLBACK_MESSAGE;
            default:
                return LocalTransactionState.UNKNOW;
        }
    }

    public static Message convertToSpringMassage(
            org.apache.rocketmq.common.message.Message message) {
        MessageBuilder messageBuilder = MessageBuilder.withPayload(message.getBody())
                .setHeader(RocketMQMessageConstants.KEYS, message.getKeys())
                .setHeader(RocketMQMessageConstants.TAGS, message.getTags())
                .setHeader(RocketMQMessageConstants.TOPIC, message.getTopic())
                .setHeader(RocketMQMessageConstants.FLAG, message.getFlag())
                .setHeader(RocketMQMessageConstants.TRANSACTION_ID, message.getTransactionId())
                .setHeader(RocketMQMessageConstants.PROPERTIES, message.getProperties());

        if (message instanceof MessageExt) {
            MessageExt messageExt = (MessageExt) message;
            messageBuilder.setHeader(RocketMQMessageConstants.MESSAGE_ID, messageExt.getMsgId())
                    .setHeader(RocketMQMessageConstants.BORN_TIMESTAMP, messageExt.getBornTimestamp())
                    .setHeader(RocketMQMessageConstants.BORN_HOST, messageExt.getBornHostString())
                    .setHeader(RocketMQMessageConstants.QUEUE_ID, messageExt.getQueueId())
                    .setHeader(RocketMQMessageConstants.SYS_FLAG, messageExt.getSysFlag());
        }

        return messageBuilder.build();
    }


    public static org.apache.rocketmq.common.message.Message convertToRocketMessage(String destination, Message<?> message, ObjectMapper objectMapper, String charset) {
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


        org.apache.rocketmq.common.message.Message rocketMassage = new org.apache.rocketmq.common.message.Message(topic, tags, payloads);

        MessageHeaders headers = message.getHeaders();
        if (Objects.nonNull(headers) && !headers.isEmpty()) {
            Object keys = headers.get("KEYS");
            // if headers has 'KEYS', set rocketMQ message key
            if (!StringUtils.isEmpty(keys)) {
                rocketMassage.setKeys(keys.toString());
            }

            // set rocketMQ message flag
            Object flagObj = headers.getOrDefault("FLAG", 0);
            int flag = 0;
            try {
                flag = Integer.parseInt(flagObj + "");
            } catch (NumberFormatException e) {
                // ignore
                log.info("flag must be integer, flagObj:{}", flagObj);
            }
            rocketMassage.setFlag(flag);


            // set rocketMQ message waitStoreMsgOkObj
            Object waitStoreMsgOkObj = headers.getOrDefault("WAIT_STORE_MSG_OK", true);
            boolean waitStoreMsgOK = Boolean.TRUE.equals(waitStoreMsgOkObj);
            rocketMassage.setWaitStoreMsgOK(waitStoreMsgOK);

            // 1.exclude "KEYS", "FLAG", "WAIT_STORE_MSG_OK"
            // 2.add other properties with prefix "USERS_"
            headers.entrySet().stream()
                    .filter(entry -> !Objects.equals(entry.getKey(), "KEYS")
                            && !Objects.equals(entry.getKey(), "FLAG")
                            && !Objects.equals(entry.getKey(), "WAIT_STORE_MSG_OK"))
                    .forEach(entry -> rocketMassage.putUserProperty("USERS_" + entry.getKey(), String.valueOf(entry.getValue())));

        }

        return rocketMassage;
    }
}
