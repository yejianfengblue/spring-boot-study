package com.yejianfengblue.spring.boot.quartz;

import com.zaxxer.hikari.HikariDataSource;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * @author yejianfengblue
 */
@TestConfiguration
public class ProxyTestDataSourceConfig {

    @Bean
    public ProxyTestDataSource proxyTestDataSource(DataSourceProperties properties) {

        HikariDataSource dataSource = properties.initializeDataSourceBuilder()
                                                .type(HikariDataSource.class)
                                                .build();

        return new ProxyTestDataSource(dataSource);
    }

    @Bean
    public DatasourceProxyBeanPostProcessor datasourceProxyBeanPostProcessor() {

        return new DatasourceProxyBeanPostProcessor();
    }
}
