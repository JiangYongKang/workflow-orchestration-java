package com.github.highcumontoa.workfloworchestrationjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** 启用工作流配置项。 */
@Configuration
@EnableConfigurationProperties(WorkflowProperties.class)
public class WorkflowConfig {
}
