package org.apache.rocketmq.spring.starter.config;

import io.netty.util.internal.ConcurrentSet;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.spring.starter.core.RocketMQTemplate;
import org.springframework.beans.factory.DisposableBean;

import java.util.Set;

public class RocketMQTransactionHandlerRegistry implements DisposableBean {

    private RocketMQTemplate rocketMQTemplate;

    private final Set<String> listenerContainer = new ConcurrentSet<>();

    public RocketMQTransactionHandlerRegistry(RocketMQTemplate rocketMQTemplate) {
        this.rocketMQTemplate = rocketMQTemplate;
    }

    @Override
    public void destroy() throws Exception {
        listenerContainer.clear();
    }

    public void register(RocketMQTransactionHandler handler) throws MQClientException {
        if (listenerContainer.contains(handler.getName())) {
            throw new MQClientException(String.format("The transaction name [%s] has been defined in TransactionListener [%s]", handler.getName(), handler.getBeanName()), null);
        }
        listenerContainer.add(handler.getName());

        rocketMQTemplate.startTransactionMQProducer(handler.getName(), handler.getTransactionListener(), handler.getCheckExecutor());
    }
}
