package com.fix42.dashboard.connectors.s3;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Contributes the S3 driver to any application that depends on this module.
 *
 * <p>An application gets S3 support by adding the dependency and nothing else: the factory
 * joins the {@code SourceResolver}'s list, and a connector selects it by configuring
 * {@code source.s3}. Guarded on the SDK client being on the classpath so that a runner
 * shipping every source module still starts when one of their clients has been excluded --
 * the AWS SDK being the one most likely to be, given its size.
 */
@AutoConfiguration
@ConditionalOnClass(S3Client.class)
public class S3SourceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public S3SourceFactory s3SourceFactory() {
        return new S3SourceFactory();
    }
}
