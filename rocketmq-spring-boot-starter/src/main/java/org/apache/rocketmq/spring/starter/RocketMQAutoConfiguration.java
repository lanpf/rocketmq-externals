/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.starter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.spring.starter.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.starter.constant.RocketMQListenerContainerConst;
import org.apache.rocketmq.spring.starter.constant.RocketMQTransactionConst;
import org.apache.rocketmq.spring.starter.core.DefaultRocketMQListenerContainer;
import org.apache.rocketmq.spring.starter.core.RocketMQListener;
import org.apache.rocketmq.spring.starter.core.RocketMQTemplate;
import org.apache.rocketmq.spring.starter.enums.ConsumeMode;
import org.apache.rocketmq.spring.starter.transaction.RocketMQTransactionAnnotationProcessor;
import org.apache.rocketmq.spring.starter.transaction.RocketMQTransactionHandlerRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@ConditionalOnClass(MQClientAPIImpl.class)
@Order
@Slf4j
public class RocketMQAutoConfiguration {

    @Bean
    @ConditionalOnClass(ObjectMapper.class)
    @ConditionalOnMissingBean(name = "rocketMQMessageObjectMapper")
    public ObjectMapper rocketMQMessageObjectMapper() {
        return new ObjectMapper();
    }

    @Bean
    @ConditionalOnClass(DefaultMQProducer.class)
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = {"name-server", "producer.group"})
    @Order(1)
    public DefaultMQProducer mqProducer(RocketMQProperties rocketMQProperties) {

        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        Assert.hasText(producerConfig.getGroup(), "[spring.rocketmq.defaultMQProducer.group] must not be null");

        DefaultMQProducer producer = new DefaultMQProducer(producerConfig.getGroup());
        producer.setNamesrvAddr(rocketMQProperties.getNameServer());
        producer.setSendMsgTimeout(producerConfig.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(producerConfig.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMsgBodyOverHowmuch());
        producer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryAnotherBrokerWhenNotStoreOk());

        return producer;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(name = "rocketMQTemplate")
    @Order(2)
    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer mqProducer,
        @Autowired(required = false) @Qualifier("rocketMQMessageObjectMapper") ObjectMapper objectMapper) {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setDefaultMQProducer(mqProducer);
        if (Objects.nonNull(objectMapper)) {
            rocketMQTemplate.setObjectMapper(objectMapper);
        }

        return rocketMQTemplate;
    }


    @Configuration
    @ConditionalOnClass(DefaultMQPushConsumer.class)
    @EnableConfigurationProperties(RocketMQProperties.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = "name-server")
    @Order
    public static class ListenerContainerConfiguration implements BeanFactoryAware, InitializingBean {
        private AtomicLong counter = new AtomicLong(0);

        private ObjectMapper objectMapper;

        private DefaultListableBeanFactory beanFactory;

        @Resource
        private StandardEnvironment environment;

        @Resource
        private RocketMQProperties rocketMQProperties;

        public ListenerContainerConfiguration() {
        }

        @Autowired(required = false)
        public ListenerContainerConfiguration(
            @Qualifier("rocketMQMessageObjectMapper") ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
            this.beanFactory = (DefaultListableBeanFactory) beanFactory;
        }

        @Override
        public void afterPropertiesSet() {
            Map<String, Object> beans = this.beanFactory.getBeansWithAnnotation(RocketMQMessageListener.class);

            if (Objects.nonNull(beans)) {
                beans.forEach(this::registerContainer);
            }
        }

        private void registerContainer(String beanName, Object bean) {
            Class<?> targetClass = AopUtils.getTargetClass(bean);

            if (!RocketMQListener.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(targetClass + " is not instance of " + RocketMQListener.class.getName());
            }

            RocketMQMessageListener annotation = targetClass.getAnnotation(RocketMQMessageListener.class);

            if (annotation.consumeMode() == ConsumeMode.ORDERLY && annotation.messageModel() == MessageModel.BROADCASTING) {
                throw new BeanDefinitionValidationException("Bad annotation definition in @RocketMQMessageListener, messageModel BROADCASTING does not support ORDERLY message!");
            }

            RocketMQListener rocketMQListener = (RocketMQListener) bean;
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultRocketMQListenerContainer.class);
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_NAMESERVER, rocketMQProperties.getNameServer());
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_TOPIC, environment.resolvePlaceholders(annotation.topic()));
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_CONSUMER_GROUP, environment.resolvePlaceholders(annotation.consumerGroup()));
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_CONSUME_MODE, annotation.consumeMode());
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_CONSUME_THREAD_MAX, annotation.consumeThreadMax());
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_MESSAGE_MODEL, annotation.messageModel());
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_SELECTOR_EXPRESS, environment.resolvePlaceholders(annotation.selectorExpress()));
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_SELECTOR_TYPE, annotation.selectorType());
            beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_ROCKETMQ_LISTENER, rocketMQListener);
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue(RocketMQListenerContainerConst.PROP_OBJECT_MAPPER, objectMapper);
            }
            beanBuilder.setDestroyMethodName(RocketMQListenerContainerConst.METHOD_DESTROY);

            String containerBeanName = DefaultRocketMQListenerContainer.class.getName() + "_" + counter.incrementAndGet();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultRocketMQListenerContainer container = beanFactory.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);

            try {
                container.start();
            } catch (Exception e) {
                log.error("started container failed. {}", container, e);
                throw new RuntimeException(e);
            }

            log.info("register rocketMQ listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
        }
    }

    @Bean
    @ConditionalOnBean(RocketMQTemplate.class)
    @ConditionalOnMissingBean(RocketMQTransactionHandlerRegistry.class)
    @Order(3)
    public RocketMQTransactionHandlerRegistry transactionHandlerRegistry(RocketMQTemplate rocketMQTemplate) {
        return new RocketMQTransactionHandlerRegistry(rocketMQTemplate);
    }


    @Bean(name = RocketMQTransactionConst.ROCKETMQ_TRANSACTION_ANNOTATION_PROCESSOR_BEAN_NAME)
    @ConditionalOnBean({RocketMQTransactionHandlerRegistry.class})
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    @Order
    public RocketMQTransactionAnnotationProcessor transactionAnnotationProcessor(RocketMQTransactionHandlerRegistry transactionHandlerRegistry) {
        return new RocketMQTransactionAnnotationProcessor(transactionHandlerRegistry);
    }

}
