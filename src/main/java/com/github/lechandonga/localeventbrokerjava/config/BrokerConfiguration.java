package com.github.lechandonga.localeventbrokerjava.config;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(BrokerProperties.class)
public class BrokerConfiguration {
}
