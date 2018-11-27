package org.apache.rocketmq.spring.starter.core;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.starter.supports.RocketMQConsumerLifecycleListener;

/**
 * @author lanpengfei
 * @date 2018/11/27
 */
public interface RocketMQPushConsumerLifecycleListener extends RocketMQConsumerLifecycleListener<DefaultMQPushConsumer> {
}
