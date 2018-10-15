package org.apache.rocketmq.spring.starter;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.spring.starter.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.starter.util.ThreadFactoryBuilder;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RocketMQTransactionAnnotationProcessor implements BeanPostProcessor {
    private final Set<Class<?>> annotationCache = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

    private RocketMQTransactionHandlerRegistry transactionHandlerRegistry;

    public RocketMQTransactionAnnotationProcessor(RocketMQTransactionHandlerRegistry transactionHandlerRegistry) {
        this.transactionHandlerRegistry = transactionHandlerRegistry;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!annotationCache.contains(bean.getClass())) {
            Class<?> clazz = AopUtils.getTargetClass(bean);
            RocketMQTransactionListener annotation = clazz.getAnnotation(RocketMQTransactionListener.class);

            annotationCache.add(bean.getClass());

            try {
                processTransactionListenerAnnotation(annotation, bean);
            } catch (MQClientException e) {
                log.error("failed to process annotation " + annotation, e);
                throw new BeanCreationException("failed to process annotation " + annotation, e);
            }
        }

        return bean;
    }

    private void processTransactionListenerAnnotation(RocketMQTransactionListener annotation, Object bean) throws MQClientException {
        if (transactionHandlerRegistry == null) {
            throw new MQClientException("Bad usage of @RocketMQTransactionListener, the class must work with producer rocketMQTemplate", null);
        }
        if (!TransactionListener.class.isAssignableFrom(bean.getClass())) {
            throw new MQClientException("Bad usage of @RocketMQTransactionListener, the class must implements interface org.apache.rocketmq.client.producer.TransactionListener", null);
        }

        ThreadPoolExecutor executorService = new ThreadPoolExecutor(annotation.corePoolSize(), annotation.maximumPoolSize(),
                annotation.keepAliveTime(), annotation.timeUnit(), new LinkedBlockingDeque<>(annotation.blockingQueueSize()), (new ThreadFactoryBuilder()).setNameFormat(annotation.poolNameFormat()).build());

        RocketMQTransactionHandler transactionHandler = RocketMQTransactionHandler.builder()
                .producerGroup(annotation.txProducerGroup())
                .transactionListener((TransactionListener) bean)
                .executorService(executorService)
                .build();

        transactionHandlerRegistry.register(transactionHandler);
    }
}
