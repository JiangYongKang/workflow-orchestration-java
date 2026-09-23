package com.github.highcumontoa.workfloworchestrationjava.config;

import com.github.highcumontoa.workfloworchestrationjava.engine.ExecutionConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 把配置属性装配为引擎使用的不可变 {@link ExecutionConfig}。 */
@Configuration
@EnableConfigurationProperties(WorkflowProperties.class)
public class EngineConfig {

    @Bean
    public ExecutionConfig executionConfig(WorkflowProperties properties) {
        return new ExecutionConfig(
                properties.getMaxConcurrency(),
                properties.getMaxAttempts(),
                properties.getRetryBackoffMillis(),
                properties.getTaskTimeoutMillis());
    }
}
