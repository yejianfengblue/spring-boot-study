package com.yejianfengblue.spring.boot.tx;

import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

@TestConfiguration
public class ProxyTestDataSourceConfig {

    @Bean
    public ProxyTestDataSource proxyTestDataSource() {

        EmbeddedDatabase actualDataSource = (new EmbeddedDatabaseBuilder())
                .setType(EmbeddedDatabaseType.H2)
                .setName("testDataSource")
                .build();

        return new ProxyTestDataSource(actualDataSource);
    }

    @Bean
    public DatasourceProxyBeanPostProcessor datasourceProxyBeanPostProcessor() {

        return new DatasourceProxyBeanPostProcessor();
    }
}
