package com.yejianfengblue.spring.boot.quartz;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Quartz JDBC job store
 */
@SpringBootTest(
    // classes is optional, because the non-private nested @Configuration will be used
    // before fallback to @SpringBootConfiguration/@SpringBootApplication
    // classes = QuartzJdbcStoreTest.Config.class,
    properties = {
                   "spring.quartz.job-store-type=jdbc",
//                   "spring.quartz.jdbc.initialize-schema=always",
                   "spring.datasource.url=jdbc:h2:tcp://localhost:22010/~/h2/quartz",
                   "spring.datasource.username=quartz",
                   "spring.datasource.password=quartz",
    })
//@Import(ProxyTestDataSourceConfig.class)
@EnableAutoConfiguration
@Slf4j
class QuartzJdbcStoreTest {

    @Autowired
    SchedulerFactoryBean schedulerFactoryBean;

    Scheduler scheduler;

    @Configuration
    static class Config {}

    @BeforeEach
    @SneakyThrows
    void setup() {
        this.scheduler = schedulerFactoryBean.getScheduler();
        log.info("Test start");
    }

    @AfterEach
    @SneakyThrows
    void clear() {
        log.info("Test end");
        this.scheduler.shutdown();
    }

    public static class CounterJob implements Job {

        private static AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("{} - {}", context.getJobDetail().getKey(), counter.incrementAndGet());
        }
    }

    @Test
    @DisplayName("When scheduler.scheduleJob(jobDetail, trigger), then ok")
    @SneakyThrows
    void scheduleJobWithJobDetailAndTrigger() {

        JobDetail jobDetail = JobBuilder.newJob(CounterJob.class)
                                        .storeDurably()
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                                           .simpleSchedule()
                                                                           .repeatForever()
                                                                           .withIntervalInSeconds(2))
                                        .build();
        scheduler.deleteJobs(List.copyOf(scheduler.getJobKeys(GroupMatcher.anyGroup())));

//         scheduler.scheduleJob(jobDetail, trigger);

        Thread.sleep(10_000);

    }
}
