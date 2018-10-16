package org.apache.rocketmq.spring.starter.transaction;

import lombok.Builder;
import lombok.Data;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.springframework.beans.factory.BeanFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lanpengfei
 */
@Builder
@Data
public class RocketMQTransactionHandler {
    private String producerGroup;

    private TransactionListener transactionListener;

    private ThreadPoolExecutor checkExecutor;
}
