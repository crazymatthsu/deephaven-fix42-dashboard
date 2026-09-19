package com.fix42.dashboard.connectors.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Contributes the Kafka driver to any application that depends on this module.
 *
 * <p>An application gets Kafka support by adding the dependency and nothing else: the factory
 * joins the {@code SourceResolver}'s list, and a connector selects it by configuring
 * {@code source.kafka}. Guarded on the consumer client being on the classpath so that a runner
 * shipping every source module still starts when one of their clients has been excluded.
 */
@AutoConfiguration
@ConditionalOnClass(KafkaConsumer.class)
public class KafkaSourceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public KafkaSourceFactory kafkaSourceFactory() {
        return new KafkaSourceFactory();
    }
}
