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

package org.apache.rocketmq.spring.starter.annotation;

import org.apache.rocketmq.spring.starter.constant.RocketMQTransactionConst;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * This annotation is used over a class which implements interface
 * org.apache.rocketmq.client.producer.TransactionListener. The class implements
 * two methods for process callback events after the txProducer sends a transactional message.
 *
 * <p>Note: The annotation is used only on RocketMQ client producer side, it can not be used
 * on consumer side.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface RocketMQTransactionListener {

    /**
     * Declare the transactionProducerGroup that is used to relate callback event to the listener, rocketMQTemplate must send a
     * transactional message with the declared transactionProducerGroup.
     *
     * <p>It is suggested to use the default transactionProducerGroup if your system only needs to define a TransactionListener class.
     */
    String txProducerGroup() default RocketMQTransactionConst.ROCKETMQ_TRANSACTION_GROUP;

    /**
     * See ThreadPoolExecutor corePoolSize
     */
    int corePoolSize() default 1;

    /**
     * See ThreadPoolExecutor maximumPoolSize
     */
    int maximumPoolSize() default 1;

    /**
     * See ThreadPoolExecutor keepAliveTime, hold 60s
     */
    long keepAliveTime() default 1000 * 60;

    /**
     * See ThreadPoolExecutor timeUnit
     */
    TimeUnit timeUnit() default TimeUnit.MILLISECONDS;

    /**
     * See ThreadPoolExecutor new LinkedBlockingDeque<>(blockingQueueSize)
     */
    int blockingQueueSize() default 2000;

    /**
     * See ThreadPoolExecutor namedFactory
     */
    String poolNameFormat() default RocketMQTransactionConst.ROCKETMQ_TRANSACTION_POOL_NAME_FORMAT;
}
