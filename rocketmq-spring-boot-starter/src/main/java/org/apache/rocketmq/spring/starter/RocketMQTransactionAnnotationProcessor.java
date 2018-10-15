package org.apache.rocketmq.spring.starter;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.spring.starter.annotation.RocketMQTransactionListener;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RocketMQTransactionAnnotationProcessor implements BeanPostProcessor {

    private final Set<Class<?>> annotationClasses =
            Collections.newSetFromMap(new ConcurrentHashMap<>(64));

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
        if (!annotationClasses.contains(bean.getClass())) {
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            RocketMQTransactionListener transactionListener = AnnotationUtils.findAnnotation(targetClass, RocketMQTransactionListener.class);

            annotationClasses.add(bean.getClass());
            if (transactionListener == null) {
                log.trace("No @RocketMQTransactionListener annotations found on bean type: {}", bean.getClass());
            } else {
                try {
                    processTransactionListenerAnnotation(transactionListener, bean);
                } catch (MQClientException e) {
                    log.error("failed to process annotation " + transactionListener, e);
                    throw new BeanCreationException("failed to process annotation " + transactionListener, e);
                }
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
        RocketMQTransactionHandler transactionHandler = RocketMQTransactionHandler.builder()
                .producerGroup(annotation.txProducerGroup())
                .transactionListener((TransactionListener) bean)
                .executorService(new ThreadPoolExecutor(annotation.corePoolSize(), annotation.maxPoolSize(),
                        annotation.keepAliveTime(), TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(annotation.blockingQueueSize())))
                .build();

        transactionHandlerRegistry.register(transactionHandler);
    }
}
