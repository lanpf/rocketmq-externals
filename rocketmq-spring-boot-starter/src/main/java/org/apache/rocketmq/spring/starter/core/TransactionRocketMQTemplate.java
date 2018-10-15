package org.apache.rocketmq.spring.starter.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.starter.util.RocketMQMessageUtil;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * @author lanpengfei
 * @date 2018/10/14
 */
@Slf4j
public class TransactionRocketMQTemplate extends RocketMQTemplate {
    private final Map<String, TransactionMQProducer> txProducerCache = new ConcurrentHashMap<>();

    @Override
    public void destroy() {
        super.destroy();
        txProducerCache.forEach((k, v) -> v.shutdown());
        txProducerCache.clear();
    }


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
