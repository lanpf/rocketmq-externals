package org.apache.rocketmq.spring.starter.core;

/**
 * @author lanpengfei
 * @date 2018/11/27
 */
public enum RocketMQLocalTransactionState {
    /**
     *
     */
    COMMIT_MESSAGE,
    ROLLBACK_MESSAGE,
    UNKNOW
}
