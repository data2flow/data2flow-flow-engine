package net.java21.data2flow.flow.runtime;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.common.PeriodicWorker;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.TimerKind;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.DebugSink;
import net.java21.data2flow.flow.runtime.repository.PartitionProgressRepository;
import net.java21.data2flow.flow.runtime.service.FlowExecutor;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import net.java21.data2flow.flow.runtime.service.JdbcExecutionStore;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * FLW-05.02 TC-FLW-097: 엔진 인스턴스 2개(같은 PostgreSQL을 쓰는 실행 문맥 2개)가 만기가 같은 지속 타이머 1,000개를 동시에 가져가도
 * {@code SELECT … FOR UPDATE SKIP LOCKED}로 각 타이머는 정확히 한 번 발화한다: 아웃박스 1,000행·중복 0, 두 인스턴스 처리 수 합 = 1,000.
 * 별도 JVM으로 kill -9 하는 시험은 {@code TimerFailoverIT}.
 */
class DurationTimerIT extends IntegrationTestSupport {

    @Autowired
    FlowRegistry registry;
    @Autowired
    FlowExecutor executor;
    @Autowired
    JdbcExecutionStore store;
    @Autowired
    PartitionProgressRepository progress;
    @Autowired
    TimerRepository timers;
    @Autowired
    net.java21.data2flow.flow.runtime.repository.BufferedTriggerRepository buffered;
    @Autowired
    TransactionTemplate tx;
    @Autowired
    DeploymentScope scope;
    @Autowired
    FlowEngineProperties properties;
    @Autowired
    JdbcClient jdbc;
    @Autowired
    @Qualifier("timerPoller")
    PeriodicWorker timerPoller;

    @Test
    @DisplayName("[FLW-05.02] TC-FLW-097 인스턴스 2개 × 만기 같은 타이머 1,000개 → 각 1회 발화, 아웃박스 1,000행·중복 0, 처리 수 합 1,000")
    void thousandTimersTwoInstances() throws Exception {
        UUID flow = UUID.randomUUID();
        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, 1, "ACTIVE", FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["9999"]}}},
                  {"id":"n-dly00001","type":"flow.delay","config":{"duration":"PT1S"}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"9998"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"n-dly00001"},{"from":"n-dly00001","to":"n-act00001"}]}"""));
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).isPresent());
        timerPoller.stop();   // 서비스 자체 폴러를 멈추고 시험의 두 인스턴스만 발화한다
        try {
            Instant due = Instant.now(Clock.systemUTC()).minusSeconds(1);
            for (int i = 0; i < 1000; i++) {
                ObjectNode context = Jsons.object();
                context.put("triggerMessageId", "timer-it-" + i);
                context.set("message", Jsons.object().put("messageId", "timer-it-" + i).put("seq", i));
                timers.insert(FlowFixtures.ORG, flow, 1, "n-dly00001", "device:" + (i % 50), TimerKind.DELAY, due, context, due);
            }
            FlowRegistry second = new FlowRegistry();
            second.put(registry.get(flow).orElseThrow());
            FlowRuntimeService instanceA = new FlowRuntimeService(registry, executor, store, progress, timers, buffered, tx, DebugSink.NONE,
                    scope, properties, Clock.systemUTC(), new SimpleMeterRegistry(), null, null);
            FlowRuntimeService instanceB = new FlowRuntimeService(second, executor, store, progress, timers, buffered, tx, DebugSink.NONE,
                    scope, properties, Clock.systemUTC(), new SimpleMeterRegistry(), null, null);

            AtomicBoolean stop = new AtomicBoolean();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CompletableFuture<Integer> a = CompletableFuture.supplyAsync(() -> drain(instanceA, timers, flow, stop), pool);
            CompletableFuture<Integer> b = CompletableFuture.supplyAsync(() -> drain(instanceB, timers, flow, stop), pool);
            int firedA = a.get();
            int firedB = b.get();
            pool.shutdown();

            long outbox = jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_outboxes WHERE flow_id = :f").param("f", flow)
                    .query(Long.class).single();
            long distinct = jdbc.sql("SELECT count(DISTINCT trigger_message_id) FROM data2flow_flow.flow_outboxes WHERE flow_id = :f")
                    .param("f", flow).query(Long.class).single();
            long fired = jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_timers WHERE flow_id = :f AND status = 'FIRED'")
                    .param("f", flow).query(Long.class).single();
            System.out.printf("[TC-FLW-097] 인스턴스 A %d, B %d, 합 %d, 아웃박스 %d행(서로 다른 원인 %d), FIRED %d%n", firedA, firedB,
                    firedA + firedB, outbox, distinct, fired);

            assertThat(firedA + firedB).isEqualTo(1000);
            assertThat(outbox).isEqualTo(1000);
            assertThat(distinct).isEqualTo(1000);
            assertThat(fired).isEqualTo(1000);
            assertThat(firedA).as("두 인스턴스가 나눠 가짐").isPositive();
            assertThat(firedB).isPositive();
        } finally {
            timerPoller.start();
        }
    }

    /** 대기 타이머가 남지 않을 때까지 발화한다(후보는 잠그지 않고 고르므로 두 인스턴스가 같은 후보를 보고, 행 잠금이 한 번만 발화시킨다) */
    private static int drain(FlowRuntimeService runtime, TimerRepository timers, UUID flow, AtomicBoolean stop) {
        int total = 0;
        while (timers.countWaiting(FlowFixtures.ORG, flow) > 0) {
            total += runtime.fireDueTimers(20, stop::get);
        }
        return total;
    }
}
