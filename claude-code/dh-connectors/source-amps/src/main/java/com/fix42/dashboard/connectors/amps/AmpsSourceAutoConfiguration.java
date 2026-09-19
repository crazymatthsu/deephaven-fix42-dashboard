package com.fix42.dashboard.connectors.amps;

import com.crankuptheamps.client.Client;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Contributes the AMPS driver to any application that depends on this module.
 *
 * <p>An application gets AMPS support by adding the dependency and nothing else: the factory
 * joins the {@code SourceResolver}'s list, and a connector selects it by configuring
 * {@code source.amps}. Guarded on the 60East client being on the classpath so that a runner
 * shipping every source module still starts when one of their clients has been excluded.
 */
@AutoConfiguration
@ConditionalOnClass(Client.class)
public class AmpsSourceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AmpsSourceFactory ampsSourceFactory() {
        return new AmpsSourceFactory();
    }
}
