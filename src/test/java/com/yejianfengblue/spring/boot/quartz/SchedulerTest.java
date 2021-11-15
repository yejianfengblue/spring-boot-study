package com.yejianfengblue.spring.boot.quartz;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@SpringBootTest
@Slf4j
class SchedulerTest {

    SchedulerFactory schedulerFactory = new StdSchedulerFactory();

    Scheduler scheduler;

    @BeforeEach
    @SneakyThrows
    void setup() {
        this.scheduler = schedulerFactory.getScheduler();
        this.scheduler.start();
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
            log.info(String.valueOf(counter.incrementAndGet()));
        }
    }

    @Test
    @DisplayName("When scheduler.scheduleJob(jobDetail, trigger), then ok")
    @SneakyThrows
    void scheduleJobWithJobDetailAndTrigger() {

        JobDetail jobDetail = JobBuilder.newJob(CounterJob.class)
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        scheduler.scheduleJob(jobDetail, trigger);

        Thread.sleep(10_000);

    }

    @Test
    @DisplayName("When scheduler.scheduleJob(trigger), then JobPersistenceException")
    void whenScheduleJobWithTrigger_throwsJobPersistenceException() {

        JobDetail jobDetail = JobBuilder.newJob(CounterJob.class)
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        assertThatThrownBy(() -> scheduler.scheduleJob(trigger))
                .isInstanceOf(JobPersistenceException.class)
                .hasMessage("The job (%s) referenced by the trigger does not exist.", jobDetail.getKey());

    }

    @Test
    @DisplayName("Given durable job added to scheduler, when scheduler.schedule(trigger), then ok")
    @SneakyThrows
    void givenDurableJobAddedToScheduler_whenScheduleWithTrigger_thenOk() {

        JobDetail jobDetail = JobBuilder.newJob(CounterJob.class)
                                        .storeDurably()
                                        .build();
        scheduler.addJob(jobDetail, true);

        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        scheduler.scheduleJob(trigger);

        Thread.sleep(10_000);

    }

    @Test
    @DisplayName("After non-durable job completed, then jobDetail and trigger don't exist in scheduler")
    @SneakyThrows
    void afterNonDurableJobCompleted_thenJobDetailAndTriggerNotExistInScheduler() {

        JobDetail jobDetail = JobBuilder.newJob(CounterJob.class)
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        scheduler.scheduleJob(jobDetail, trigger);
        assertThat(scheduler.checkExists(jobDetail.getKey()))
                .isTrue();
        assertThat(scheduler.getJobKeys(GroupMatcher.anyGroup()))
                .hasSize(1);
        assertThat(scheduler.checkExists(trigger.getKey()))
                .isTrue();
        assertThat(scheduler.getTriggerKeys(GroupMatcher.anyGroup()))
                .hasSize(1);

        Thread.sleep(20_000);

        assertThat(scheduler.checkExists(jobDetail.getKey()))
                .isFalse();
        assertThat(scheduler.getJobKeys(GroupMatcher.anyGroup()))
                .isEmpty();
        assertThat(scheduler.checkExists(trigger.getKey()))
                .isFalse();
        assertThat(scheduler.getTriggerKeys(GroupMatcher.anyGroup()))
                .isEmpty();

    }

    @Test
    @DisplayName("Before first trigger ends, when schedule same jobDetail with new trigger, then ok")
    @SneakyThrows
    void BeforeFirstTriggerEnds_whenScheduleSameJobDetailWithNewTrigger_thenOk() {

        // At the beginning, scheduler doesn't contain job or trigger
        assertThat(scheduler.getJobKeys(GroupMatcher.anyGroup()))
                .isEmpty();
        assertThat(scheduler.getTriggerKeys(GroupMatcher.anyGroup()))
                .isEmpty();

        // Schedule jobDetail and trigger
        JobDetail jobDetail = JobBuilder.newJob(CounterJob.class)
                                        .build();
        log.info("Job key = {}", jobDetail.getKey());
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        log.info("Trigger key = {}", trigger.getKey());
        scheduler.scheduleJob(jobDetail, trigger);
        log.info("Action: schedule job {} with trigger {}", jobDetail.getKey(), trigger.getKey());
        log.info("\n\n\n");

        assertThat(scheduler.checkExists(jobDetail.getKey()))
                .isTrue();
        assertThat(scheduler.getJobKeys(GroupMatcher.anyGroup()))
                .hasSize(1);
        assertThat(scheduler.checkExists(trigger.getKey()))
                .isTrue();
        assertThat(scheduler.getTriggerKeys(GroupMatcher.anyGroup()))
                .hasSize(1);
        log.info("Scheduler job keys = {}", scheduler.getJobKeys(GroupMatcher.anyGroup()));
        log.info("Scheduler trigger keys = {}", scheduler.getTriggerKeys(GroupMatcher.anyGroup()));
        log.info("\n\n\n");

        Thread.sleep(3_000);
        // Before the first trigger ends, the jobDetail still exists in scheduler.
        // Schedule with the same jobDetail and a new trigger using scheduler.scheduleJob(trigger2)
        Trigger trigger2 = TriggerBuilder.newTrigger()
                                         .forJob(jobDetail)
                                         .withSchedule(SimpleScheduleBuilder
                                                               .simpleSchedule()
                                                               .withRepeatCount(2)
                                                               .withIntervalInSeconds(10))
                                         .build();
        scheduler.scheduleJob(trigger2);
        log.info("Action: schedule job {} with trigger {}", jobDetail.getKey(), trigger2.getKey());
        log.info("\n\n\n");

        assertThat(scheduler.checkExists(jobDetail.getKey()))
                .isTrue();
        assertThat(scheduler.getJobKeys(GroupMatcher.anyGroup()))
                .hasSize(1);
        assertThat(scheduler.checkExists(trigger.getKey()))
                .isTrue();
        assertThat(scheduler.checkExists(trigger2.getKey()))
                .isTrue();
        assertThat(scheduler.getTriggerKeys(GroupMatcher.anyGroup()))
                .hasSize(2);
        log.info("Scheduler job keys = {}", scheduler.getJobKeys(GroupMatcher.anyGroup()));
        log.info("Scheduler trigger keys = {}", scheduler.getTriggerKeys(GroupMatcher.anyGroup()));
        log.info("\n\n\n");

        Thread.sleep(30_000);
        log.info("All triggers should end");
        assertThat(scheduler.getJobKeys(GroupMatcher.anyGroup()))
                .isEmpty();
        assertThat(scheduler.getTriggerKeys(GroupMatcher.anyGroup()))
                .isEmpty();
        log.info("Scheduler job keys = {}", scheduler.getJobKeys(GroupMatcher.anyGroup()));
        log.info("Scheduler trigger keys = {}", scheduler.getTriggerKeys(GroupMatcher.anyGroup()));

    }

    @DisallowConcurrentExecution
    public static class NonConcurrentSleepJob implements Job {

        private static AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {

            log.info("Trigger key = {}", context.getTrigger().getKey());
            log.info("{} Sleep 5s...", counter.incrementAndGet());

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @DisplayName("Given non-concurrent job, before first trigger ends, " +
                 "when new trigger added to same jobDetail, " +
                 "then latter trigger may fire before first trigger ends")
    @SneakyThrows
    void nonConcurrentJobAndMultipleTriggers() {

        // Schedule jobDetail and trigger
        JobDetail jobDetail = JobBuilder.newJob(NonConcurrentSleepJob.class)
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        scheduler.scheduleJob(jobDetail, trigger);

        Thread.sleep(3_000);
        // Before the first trigger ends, the jobDetail still exists in scheduler.
        // Schedule with the same jobDetail and a new trigger using scheduler.scheduleJob(trigger2)
        Trigger trigger2 = TriggerBuilder.newTrigger()
                                         .forJob(jobDetail)
                                         .withSchedule(SimpleScheduleBuilder
                                                               .simpleSchedule()
                                                               .withRepeatCount(2)
                                                               .withIntervalInSeconds(10))
                                         .build();
        scheduler.scheduleJob(trigger2);

        Thread.sleep(30_000);

    }

    @Test
    @DisplayName("Given durable non-concurrent job, after first trigger ends, " +
                 "when new trigger added to same jobDetail, " +
                 "then latter trigger fire ok")
    @SneakyThrows
    void durableNonConcurrentJobAndMultipleTriggers() {

        // Schedule jobDetail and trigger
        JobDetail jobDetail = JobBuilder.newJob(NonConcurrentSleepJob.class)
                                        .storeDurably()
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        scheduler.scheduleJob(jobDetail, trigger);

        Thread.sleep(30_000);
        // After the first trigger ends, the durable jobDetail still exists in scheduler.
        // Schedule with the same jobDetail and a new trigger using scheduler.scheduleJob(trigger2)
        Trigger trigger2 = TriggerBuilder.newTrigger()
                                         .forJob(jobDetail)
                                         .withSchedule(SimpleScheduleBuilder
                                                               .simpleSchedule()
                                                               .withRepeatCount(2)
                                                               .withIntervalInSeconds(10))
                                         .build();
        scheduler.scheduleJob(trigger2);

        Thread.sleep(30_000);

    }

    @Test
    @DisplayName("Given durable non-concurrent job, " +
                 "when schedule(jobDetail, trigger) with diff triggers, " +
                 "then exception job already exists with key")
    @SneakyThrows
    void scheduleJobDetailAndTriggerMultipleTimes_thenExceptionJobAlreadyExists() {

        // Schedule jobDetail and trigger
        JobDetail jobDetail = JobBuilder.newJob(NonConcurrentSleepJob.class)
                                        .storeDurably()
                                        .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .forJob(jobDetail)
                                        .withSchedule(SimpleScheduleBuilder
                                                              .simpleSchedule()
                                                              .withRepeatCount(2)
                                                              .withIntervalInSeconds(2))
                                        .build();
        scheduler.scheduleJob(jobDetail, trigger);

        Trigger trigger2 = TriggerBuilder.newTrigger()
                                         .forJob(jobDetail)
                                         .withSchedule(SimpleScheduleBuilder
                                                               .simpleSchedule()
                                                               .withRepeatCount(2)
                                                               .withIntervalInSeconds(10))
                                         .build();
        assertThatThrownBy(() -> scheduler.scheduleJob(jobDetail, trigger2))
                .isInstanceOf(ObjectAlreadyExistsException.class)
                .hasMessage("Unable to store Job : '%s', because one already exists with this identification.",
                            jobDetail.getKey());
    }
}
