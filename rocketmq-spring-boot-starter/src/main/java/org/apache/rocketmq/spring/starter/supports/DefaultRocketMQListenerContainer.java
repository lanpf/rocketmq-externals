/*
 * Licensed to the Apache Software Foundation (ASF) under one REGEX_OR more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law REGEX_OR agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES REGEX_OR CONDITIONS OF ANY KIND, either express REGEX_OR implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.starter.supports;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.spring.starter.core.RocketMQListener;
import org.apache.rocketmq.spring.starter.core.RocketMQPushConsumerLifecycleListener;
import org.apache.rocketmq.spring.starter.enums.ConsumeMode;
import org.apache.rocketmq.spring.starter.enums.SelectorType;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
@Slf4j
@ToString
public class DefaultRocketMQListenerContainer implements InitializingBean, RocketMQListenerContainer {

    private static final String CONSUMER_GROUP_PREFIX = "cg";
    private static final String REGEX_TAGS = "\\|\\|";
    private static final String JOINER_TAGS = "||";

    @Setter
    @Getter
    private String nameServer;

    @Setter
    @Getter
    private String consumerGroup;

    @Setter
    @Getter
    private String topic;

    @Setter
    @Getter
    private SelectorType selectorType = SelectorType.TAG;

    @Setter
    @Getter
    private String selectorExpress = "*";

    @Setter
    @Getter
    private ConsumeMode consumeMode = ConsumeMode.CONCURRENTLY;

    @Setter
    @Getter
    private MessageModel messageModel = MessageModel.CLUSTERING;

    @Setter
    @Getter
    private int consumeThreadMax = 64;

    /**
     * See class ConsumeOrderlyContext
     */
    @Setter
    @Getter
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * See class ConsumeConcurrentlyContext
     *
     * Message consume retry strategy<br>
     * -1,no retry,startTransactionMQProducer into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    @Setter
    @Getter
    private int delayLevelWhenNextConsume = 0;

    @Setter
    @Getter
    private ObjectMapper objectMapper = new ObjectMapper();

    @Getter
    @Setter
    private String charset = RemotingHelper.DEFAULT_CHARSET;

    @Setter
    private RocketMQListener rocketMQListener;

    private DefaultMQPushConsumer consumer;

    private Class messageType;

    private boolean started;

    @Override
    public void destroy() {

        started = false;
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        log.info("container destroyed: {}", this);
    }

    public synchronized void start() throws MQClientException {

        if (started) {
            log.info("container already started: {}", this);
        } else {
            initConsumerGroup();
            initRocketMQPushConsumer();
            initMessageType();

            consumer.start();
            started = true;

            log.debug("msgType: {}", messageType.getName());
            log.info("start container: {}", this);
        }
    }

    private void initConsumerGroup() {
        Assert.notNull(topic, "Property 'topic' is required");
        Assert.notNull(selectorExpress, "Property 'selectorExpress' is required");

        if (!StringUtils.isEmpty(consumerGroup)) {
            return;
        }
        String[] tags = selectorExpress.replaceAll(" ", "").split(REGEX_TAGS);
        Arrays.sort(tags);

        selectorExpress = String.join(JOINER_TAGS, tags);
        consumerGroup = CONSUMER_GROUP_PREFIX + "_" + topic + "_" + selectorExpress;
    }

    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @Override
        @SuppressWarnings("unchecked")
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : messages) {
                log.debug("received msg: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    rocketMQListener.onMessage(doConvertMessage(messageExt));
                    long costTime = System.currentTimeMillis() - now;
                    log.debug("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("consume message failed. messageExt:{}", messageExt, e);
                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @Override
        @SuppressWarnings("unchecked")
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messages, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : messages) {
                log.debug("received msg: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    rocketMQListener.onMessage(doConvertMessage(messageExt));
                    long costTime = System.currentTimeMillis() - now;
                    log.info("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("consume message failed. messageExt:{}", messageExt, e);
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(MessageExt messageExt) {
        if (Objects.equals(messageType, MessageExt.class)) {
            return messageExt;
        } else {
            String message = new String(messageExt.getBody(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return message;
            } else {
                // if msgType not string, use objectMapper change it.
                try {
                    return objectMapper.readValue(message, messageType);
                } catch (Exception e) {
                    log.error("convert failed. message: {}, messageType: {}", message, messageType);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }
    }

    private void initMessageType() {
        messageType = Object.class;
        Type[] interfaces = AopUtils.getTargetClass(rocketMQListener).getGenericInterfaces();
        if (interfaces == null) {
            return;
        }
        for (Type type : interfaces) {
            if (type instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                if (Objects.equals(parameterizedType.getRawType(), RocketMQListener.class)
                        && Objects.nonNull(parameterizedType.getActualTypeArguments())
                        && parameterizedType.getActualTypeArguments().length > 0
                ) {
                    messageType = (Class) parameterizedType.getActualTypeArguments()[0];
                    return;
                }
            }
        }
    }

    private void initRocketMQPushConsumer() throws MQClientException {

        Assert.notNull(rocketMQListener, "Property 'rocketMQListener' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");

        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeThreadMax(consumeThreadMax);
        if (consumeThreadMax < consumer.getConsumeThreadMin()) {
            consumer.setConsumeThreadMin(consumeThreadMax);
        }

        consumer.setMessageModel(messageModel);

        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpress);
                break;
            case SQL92:
                consumer.subscribe(topic, MessageSelector.bySql(selectorExpress));
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        switch (consumeMode) {
            case ORDERLY:
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

        // provide an entryway to custom setting RocketMQ consumer
        if (rocketMQListener instanceof RocketMQPushConsumerLifecycleListener) {
            ((RocketMQPushConsumerLifecycleListener) rocketMQListener).prepareStart(consumer);
        }
    }

}
