package org.apache.rocketmq.spring.starter.core;

import org.springframework.messaging.Message;

/**
 * @author lanpengfei
 * @date 2018/11/27
 */
public interface RocketMQLocalTransactionListener {
    RocketMQLocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    RocketMQLocalTransactionState checkLocalTransaction(final Message msg);
}
