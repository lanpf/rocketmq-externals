package org.apache.rocketmq.spring.starter.config;

import lombok.Builder;
import lombok.Data;
import org.apache.rocketmq.spring.starter.core.RocketMQLocalTransactionListener;
import org.springframework.beans.factory.BeanFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lanpengfei
 */
@Builder
@Data
public class RocketMQTransactionHandler {
    private String name;

    private String beanName;

    private BeanFactory beanFactory;

    private RocketMQLocalTransactionListener transactionListener;

    private ThreadPoolExecutor checkExecutor;
}
