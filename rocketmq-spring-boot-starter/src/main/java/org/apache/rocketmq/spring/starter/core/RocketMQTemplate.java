/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.starter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.spring.starter.util.RocketMQMessageUtil;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.core.AbstractMessageSendingTemplate;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

@SuppressWarnings({"WeakerAccess", "unused"})
@Slf4j
public class RocketMQTemplate extends AbstractMessageSendingTemplate<String> implements InitializingBean, DisposableBean {

    @Getter
    @Setter
    private DefaultMQProducer defaultMQProducer;

    @Setter
    @Getter
    private ObjectMapper objectMapper = new ObjectMapper();

    @Getter
    @Setter
    private String charset = RemotingHelper.DEFAULT_CHARSET;

    @Getter
    @Setter
    private MessageQueueSelector messageQueueSelector = new SelectMessageQueueByHash();

    /**
     * <p> Send message in synchronous mode. This method returns only when the sending procedure totally completes.
     * Reliable synchronous transmission is used in extensive scenes, such as important notification messages, SMS
     * notification, SMS marketing system, etc.. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link DefaultMQProducer#getRetryTimesWhenSendFailed} times before claiming failure. As a result, multiple
     * messages may potentially delivered to broker(s). It's up to the application developers to resolve potential
     * duplication issue.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Message<?> message) {
        return syncSend(destination, message, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSend(String, Message)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Message<?> message, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.info("syncSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, objectMapper, charset);
            SendResult sendResult = defaultMQProducer.send(rocketMsg, timeout);
            long costTime = System.currentTimeMillis() - now;
            log.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        } catch (Exception e) {
            log.warn("syncSend failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #syncSend(String, Message)}.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Object payload) {
        return syncSend(destination, payload, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSend(String, Object)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Object payload, long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        return syncSend(destination, message, timeout);
    }

    /**
     * Same to {@link #syncSend(String, Message)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Message<?> message, String hashKey) {
        return syncSendOrderly(destination, message, hashKey, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSendOrderly(String, Message, String)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Message<?> message, String hashKey, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.warn("syncSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, objectMapper, charset);
            SendResult sendResult = defaultMQProducer.send(rocketMsg, messageQueueSelector, hashKey, timeout);
            long costTime = System.currentTimeMillis() - now;
            log.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        } catch (Exception e) {
            log.warn("syncSendOrderly failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #syncSend(String, Object)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Object payload, String hashKey) {
        return syncSendOrderly(destination, payload, hashKey, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSendOrderly(String, Object, String)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Object payload, String hashKey, long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        return syncSendOrderly(destination, message, hashKey, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    public void asyncSend(String destination, Message<?> message, SendCallback sendCallback, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.warn("asyncSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, objectMapper, charset);
            defaultMQProducer.send(rocketMsg, sendCallback, timeout);
        } catch (Exception e) {
            log.warn("asyncSend failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * <p> Send message to broker asynchronously. asynchronous transmission is generally used in response time sensitive
     * business scenarios. </p>
     *
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
     *
     * Similar to {@link #syncSend(String, Object)}, internal implementation would potentially retry up to {@link
     * DefaultMQProducer#getRetryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield
     * message duplication and application developers are the one to resolve this potential issue.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String destination, Message<?> message, SendCallback sendCallback) {
        asyncSend(destination, message, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSend(String, Object, SendCallback)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    public void asyncSend(String destination, Object payload, SendCallback sendCallback, long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        asyncSend(destination, message, sendCallback, timeout);
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)}.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String destination, Object payload, SendCallback sendCallback) {
        asyncSend(destination, payload, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Message, String, SendCallback)} with send timeout specified in
     * addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    public void asyncSendOrderly(String destination, Message<?> message, String hashKey, SendCallback sendCallback,
        long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.warn("asyncSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, objectMapper, charset);
            defaultMQProducer.send(rocketMsg, messageQueueSelector, hashKey, sendCallback, timeout);
        } catch (Exception e) {
            log.warn("asyncSendOrderly failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendOrderly(String destination, Message<?> message, String hashKey, SendCallback sendCallback) {
        asyncSendOrderly(destination, message, hashKey, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Message, String, SendCallback)}.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendOrderly(String destination, Object payload, String hashKey, SendCallback sendCallback) {
        asyncSendOrderly(destination, payload, hashKey, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Object, String, SendCallback)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    public void asyncSendOrderly(String destination, Object payload, String hashKey, SendCallback sendCallback,
        long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        asyncSendOrderly(destination, message, hashKey, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     *
     * One-way transmission is used for cases requiring moderate reliability, such as log collection.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     */
    public void sendOneWay(String destination, Message<?> message) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.warn("sendOneWay failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, objectMapper, charset);
            defaultMQProducer.sendOneway(rocketMsg);
        } catch (Exception e) {
            log.warn("sendOneWay failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #sendOneWay(String, Message)}
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     */
    public void sendOneWay(String destination, Object payload) {
        Message<?> message = this.doConvert(payload, null, null);
        sendOneWay(destination, message);
    }

    /**
     * Same to {@link #sendOneWay(String, Message)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     */
    public void sendOneWayOrderly(String destination, Message<?> message, String hashKey) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.warn("sendOneWayOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }

        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, objectMapper, charset);
            defaultMQProducer.sendOneway(rocketMsg, messageQueueSelector, hashKey);
        } catch (Exception e) {
            log.warn("sendOneWayOrderly failed. destination:{}, message:{}", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #sendOneWayOrderly(String, Message, String)}
     *
     * @param destination formats: `topicName:tags`
     * @param payload the Object to use as payload
     */
    public void sendOneWayOrderly(String destination, Object payload, String hashKey) {
        Message<?> message = this.doConvert(payload, null, null);
        sendOneWayOrderly(destination, message, hashKey);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(defaultMQProducer, "Property 'defaultMQProducer' is required");
        defaultMQProducer.start();
    }

    @Override
    protected void doSend(String destination, Message<?> message) {
        SendResult sendResult = syncSend(destination, message);
        log.debug("send message to `{}` finished. result:{}", destination, sendResult);
    }

    @Override
    protected Message<?> doConvert(Object payload, Map<String, Object> headers, MessagePostProcessor postProcessor) {
        String content;
        if (payload instanceof String) {
            content = (String) payload;
        } else {
            // if payload not as string, use objectMapper change it.
            try {
                content = objectMapper.writeValueAsString(payload);
            } catch (JsonProcessingException e) {
                log.info("convert payload to String failed. payload:{}", payload);
                throw new RuntimeException("convert to payload to String failed.", e);
            }
        }

        MessageBuilder<?> builder = MessageBuilder.withPayload(content);
        if (headers != null) {
            builder.copyHeaders(headers);
        }
        builder.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN);

        Message<?> message = builder.build();
        if (postProcessor != null) {
            message = postProcessor.postProcessMessage(message);
        }
        return message;
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(defaultMQProducer)) {
            defaultMQProducer.shutdown();
        }

        txProducerCache.forEach((k, v) -> v.shutdown());
        txProducerCache.clear();
    }

    private final Map<String, TransactionMQProducer> txProducerCache = new ConcurrentHashMap<>();

    private TransactionMQProducer stageMQProducer(String producerGroup) throws MQClientException {

        TransactionMQProducer cachedProducer = txProducerCache.get(producerGroup);
        if (cachedProducer == null) {
            throw new MQClientException(String.format("Can not found MQProducer '%s' in cache! please define @RocketMQTransactionListener class or invoke createOrGetStartedTransactionMQProducer() to create it firstly", producerGroup), null);
        }
        return cachedProducer;
    }

    public TransactionSendResult sendMessageInTransaction(final String txProducerGroup, final org.apache.rocketmq.common.message.Message rocketMsg, final Object arg) throws MQClientException {
        TransactionMQProducer txProducer = this.stageMQProducer(txProducerGroup);
        return txProducer.sendMessageInTransaction(rocketMsg, arg);
    }

    public TransactionSendResult sendMessageInTransaction(final String txProducerGroup, final String destination, final Message<?> message, final Object arg) throws MQClientException {
        TransactionMQProducer txProducer = this.stageMQProducer(txProducerGroup);
        org.apache.rocketmq.common.message.Message rocketMsg = RocketMQMessageUtil.convertToRocketMsg(destination, message, getObjectMapper(), getCharset());
        return txProducer.sendMessageInTransaction(rocketMsg, arg);
    }

    public TransactionSendResult sendMessageInTransaction(final String txProducerGroup, final String destination, final Object payload, final Object arg) throws MQClientException {
        return sendMessageInTransaction(txProducerGroup, destination, this.doConvert(payload, null, null), arg);
    }

    public void remove(String producerGroup) {
        if (txProducerCache.containsKey(producerGroup)) {
            DefaultMQProducer cachedProducer = txProducerCache.get(producerGroup);
            cachedProducer.shutdown();
            txProducerCache.remove(producerGroup);
        }
    }

    public void put(String producerGroup, TransactionListener transactionListener, ExecutorService executorService) throws MQClientException {
        if (!txProducerCache.containsKey(producerGroup)) {
            TransactionMQProducer txProducer = initTransactionMQProducer(producerGroup, transactionListener, executorService);
            txProducer.start();
            txProducerCache.put(producerGroup, txProducer);
        }

    }

    private TransactionMQProducer initTransactionMQProducer(String producerGroup, TransactionListener transactionListener, ExecutorService executorService) {
        DefaultMQProducer defaultMQProducer = getDefaultMQProducer();
        Assert.notNull(defaultMQProducer, "Property 'defaultMQProducer' is required");
        Assert.notNull(transactionListener, "Parameter 'transactionListener' is required");

        //TODO: Support RPCHook
        TransactionMQProducer txProducer = new TransactionMQProducer(producerGroup);
        txProducer.setTransactionListener(transactionListener);

        txProducer.setNamesrvAddr(getDefaultMQProducer().getNamesrvAddr());
        if (executorService != null) {
            txProducer.setExecutorService(executorService);
        }

        txProducer.setSendMsgTimeout(defaultMQProducer.getSendMsgTimeout());
        txProducer.setRetryTimesWhenSendFailed(defaultMQProducer.getRetryTimesWhenSendFailed());
        txProducer.setRetryTimesWhenSendAsyncFailed(defaultMQProducer.getRetryTimesWhenSendAsyncFailed());
        txProducer.setMaxMessageSize(defaultMQProducer.getMaxMessageSize());
        txProducer.setCompressMsgBodyOverHowmuch(defaultMQProducer.getCompressMsgBodyOverHowmuch());
        txProducer.setRetryAnotherBrokerWhenNotStoreOK(defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK());

        return txProducer;
    }
}
