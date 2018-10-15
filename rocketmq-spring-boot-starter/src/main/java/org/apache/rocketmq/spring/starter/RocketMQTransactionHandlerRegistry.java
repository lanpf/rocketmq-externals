package org.apache.rocketmq.spring.starter;

import io.netty.util.internal.ConcurrentSet;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.spring.starter.core.TransactionRocketMQTemplate;
import org.springframework.beans.factory.DisposableBean;

import java.util.Set;

public class RocketMQTransactionHandlerRegistry implements DisposableBean {

    private TransactionRocketMQTemplate transactionRocketMQTemplate;

    public RocketMQTransactionHandlerRegistry(TransactionRocketMQTemplate transactionRocketMQTemplate) {
        this.transactionRocketMQTemplate = transactionRocketMQTemplate;
    }

    private final Set<String> listenerContainer = new ConcurrentSet<>();

    @Override
    public void destroy() throws Exception {
        listenerContainer.clear();
    }

    public void register(RocketMQTransactionHandler handler) throws MQClientException {
        if (listenerContainer.contains(handler.getProducerGroup())) {
            throw new MQClientException(String.format("The transaction name [%s] has been defined in TransactionListener", handler.getProducerGroup()), null);
        }
        listenerContainer.add(handler.getProducerGroup());

        transactionRocketMQTemplate.put(handler.getProducerGroup(), handler.getTransactionListener(), handler.getExecutorService());
    }
}
