package org.apache.rocketmq.spring.starter;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.spring.starter.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.starter.config.RocketMQTransactionHandler;
import org.apache.rocketmq.spring.starter.config.RocketMQTransactionHandlerRegistry;
import org.apache.rocketmq.spring.starter.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.starter.supports.ThreadFactoryBuilder;
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

@Slf4j
public class RocketMQTransactionAnnotationProcessor implements BeanPostProcessor, BeanFactoryAware {
    private BeanFactory beanFactory;
    private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();
    private final Set<Class<?>> filter = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

    private RocketMQTransactionHandlerRegistry transactionHandlerRegistry;

    public RocketMQTransactionAnnotationProcessor(RocketMQTransactionHandlerRegistry transactionHandlerRegistry) {
        this.transactionHandlerRegistry = transactionHandlerRegistry;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
        }
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!filter.contains(bean.getClass())) {
            filter.add(bean.getClass());
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            RocketMQTransactionListener transactionListener = AnnotationUtils.getAnnotation(targetClass, RocketMQTransactionListener.class);
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

    private void processTransactionListenerAnnotation(RocketMQTransactionListener transactionListener, Object bean) throws MQClientException {
        if (transactionHandlerRegistry == null) {
            throw new MQClientException("Bad usage of @RocketMQTransactionListener, the class must work with producer rocketMQTemplate", null);
        }
        if (!RocketMQLocalTransactionListener.class.isAssignableFrom(bean.getClass())) {
            throw new MQClientException("Bad usage of @RocketMQTransactionListener, the class must implements interface org.apache.rocketmq.client.producer.TransactionListener", null);
        }

        ThreadPoolExecutor checkExecutor = new ThreadPoolExecutor(transactionListener.corePoolSize(), transactionListener.maximumPoolSize(),
                transactionListener.keepAliveTime(), transactionListener.timeUnit(), new LinkedBlockingDeque<>(transactionListener.blockingQueueSize()), (new ThreadFactoryBuilder()).setNameFormat(transactionListener.poolNameFormat()).build());

        RocketMQTransactionHandler transactionHandler = RocketMQTransactionHandler.builder()
                .name(transactionListener.txProducerGroup())
                .beanName(bean.getClass().getName())
                .beanFactory(this.beanFactory)
                .transactionListener((RocketMQLocalTransactionListener) bean)
                .checkExecutor(checkExecutor)
                .build();

        transactionHandlerRegistry.register(transactionHandler);
    }
}
