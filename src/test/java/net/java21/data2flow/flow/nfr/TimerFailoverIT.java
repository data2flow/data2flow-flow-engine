package net.java21.data2flow.flow.nfr;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.support.CoreApiStub;
import net.java21.data2flow.flow.support.FlowEngineProcess;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.TestInfrastructure;
import net.java21.data2flow.flow.support.TestStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * <b>M3 완료 확인</b>(plan/milestones.md §M3): "flow-engine 두 대 중 하나를 kill -9 해도 지속 타이머가 한 번만 발화".
 * FLW-05.02 AT-FLW-24.2(TC-FLW-097 성격의 다중 인스턴스)·TC-FLW-098: flow-engine을 <b>별도 JVM 두 개</b>로 띄우고(같은 PostgreSQL·RabbitMQ,
 * Single Active Consumer) 지속 판정({@code for: PT8S}, 운영의 5분을 줄인 것) 도중에 프로세스를 SIGKILL 한다.
 *
 * <ol>
 *   <li>파티션을 맡아 대기를 시작한 인스턴스를 대기 3초째에 kill -9 → 다른 인스턴스가 만기에 정확히 한 번 발화, 아웃박스 1행,
 *       {@code action.commands} 1건, 타이머는 처음부터 다시 세지 않음(만들고 8초 뒤 발화)</li>
 *   <li>타이머를 잠그고 발화 트랜잭션 도중(아웃박스 INSERT 대기)인 인스턴스를 kill -9 → 잠금이 풀리고 남은 인스턴스가 한 번만 발화,
 *       아웃박스 1행·발행 1건</li>
 * </ol>
 * 다른 시험과 섞이지 않도록 별도 vhost {@code flow-nfr}·조직 77·별도 core 대역을 쓴다.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TimerFailoverIT {

    private static final String VHOST = "flow-nfr";
    private static final long ORG = 77;
    private static final UUID FLOW = UUID.fromString("77000000-0000-4000-8000-000000000077");
    private static final Clock WALL = Clock.systemUTC();
    private static final Duration FOR = Duration.ofSeconds(8);

    private static CoreApiStub core;
    private static TestStreams streams;
    private static JdbcClient jdbc;
    private static FlowEngineProcess a;
    private static FlowEngineProcess b;
    private static FlowEngineProcess c;

    @BeforeAll
    static void startInstances() {
        TestInfrastructure.createVhost(VHOST);
        jdbc = JdbcClient.create(new DriverManagerDataSource(TestInfrastructure.POSTGRES.getJdbcUrl(),
                TestInfrastructure.POSTGRES.getUsername(), TestInfrastructure.POSTGRES.getPassword()));
        core = new CoreApiStub().organization(ORG);
        core.put(FLOW, ORG, 1, "ACTIVE", FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["7701","7702"]},"metrics":["temperature"]}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":27,"for":"PT8S"}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"spaceId":"77","relation":"controls","capability":"Thermostat"},
                    "capability":"Thermostat","command":"set","args":{"mode":"cool","targetTemperature":24}}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""));
        streams = new TestStreams(VHOST, 3);
        a = FlowEngineProcess.start("flow-nfr-a", VHOST, core.baseUrl());
        a.awaitReady(Duration.ofMinutes(3));
        b = FlowEngineProcess.start("flow-nfr-b", VHOST, core.baseUrl());
        b.awaitReady(Duration.ofMinutes(3));
        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.sql(
                "SELECT count(*) FROM data2flow_flow.flow_instance_versions WHERE flow_id = :f AND applied_version = 1")
                .param("f", FLOW).query(Long.class).single() == 2);
    }

    @AfterAll
    static void stopInstances() {
        for (FlowEngineProcess p : new FlowEngineProcess[]{a, b, c}) {
            if (p != null) {
                p.close();
            }
        }
        if (streams != null) {
            streams.close();
        }
        if (core != null) {
            core.close();
        }
    }

    private static CanonicalTelemetry hot(long device) {
        Instant now = Instant.now(WALL);
        return CanonicalTelemetry.builder().organizationId(ORG).sourceId(3).externalId("nfr-" + device).deviceId(device)
                .modelId("em300-th").spaceId(77L).measuredAt(now).receivedAt(now)
                .metric(CanonicalTelemetry.Metric.of("temperature", 28.5, "℃")).rawMessageId(1).build();
    }

    private static Map<String, Object> timer(String targetKey) {
        return jdbc.sql("""
                        SELECT status, created_at, due_at, fired_at FROM data2flow_flow.flow_timers
                         WHERE flow_id = :f AND target_key = :k ORDER BY id DESC LIMIT 1""")
                .param("f", FLOW).param("k", targetKey).query().listOfRows().stream().findFirst().orElse(Map.of());
    }

    private static long outbox(String key) {
        return jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_outboxes WHERE idempotency_key = :k").param("k", key)
                .query(Long.class).single();
    }

    private static List<ActionRequest> commands(String key) {
        return streams.commands(FLOW).stream().filter(r -> r.idempotencyKey().equals(key)).toList();
    }

    /** 파티션을 지금 맡고 있는 인스턴스(로그의 마지막 활성·비활성 기록) */
    private static Optional<FlowEngineProcess> owner(int partition, FlowEngineProcess... candidates) {
        String stream = "data2flow.telemetry-" + partition;
        for (FlowEngineProcess p : candidates) {
            String log = p.logText();
            int active = log.lastIndexOf("파티션 " + stream + " 활성:");
            int inactive = log.lastIndexOf("파티션 " + stream + " 비활성");
            if (active >= 0 && active > inactive) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    @Test
    @Order(1)
    @DisplayName("[FLW-05.02][AT-FLW-24.2] M3 완료 확인: 대기 3초째(8초 지속) 파티션 담당 인스턴스 kill -9 → 다른 인스턴스가 만기에 정확히 한 번 발화, 아웃박스 1행·발행 1건")
    void killOwnerWhilePending() {
        CanonicalTelemetry t = hot(7701);
        String key = ActionIdempotencyKeys.flow(FLOW.toString(), "n-act00001", t.messageId().toString());

        streams.publish(t);
        await().atMost(Duration.ofSeconds(20)).until(() -> "WAITING".equals(timer("device:7701").get("status")));
        int partition = jdbc.sql("SELECT stream_partition FROM data2flow_flow.flow_partition_progress WHERE flow_id = :f")
                .param("f", FLOW).query(Integer.class).single();
        FlowEngineProcess victim = owner(partition, a, b).orElse(a);
        FlowEngineProcess survivor = victim == a ? b : a;
        Instant created = ((java.sql.Timestamp) timer("device:7701").get("created_at")).toInstant();
        await().atMost(Duration.ofSeconds(10)).until(() -> Duration.between(created, Instant.now(WALL)).toMillis() >= 3_000);

        victim.kill();
        Instant killedAt = Instant.now(WALL);

        await().atMost(Duration.ofSeconds(30)).until(() -> "FIRED".equals(timer("device:7701").get("status")));
        await().atMost(Duration.ofSeconds(20)).until(() -> commands(key).size() == 1);
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6)).until(() -> commands(key).size() == 1 && outbox(key) == 1);
        Instant fired = ((java.sql.Timestamp) timer("device:7701").get("fired_at")).toInstant();
        Duration elapsed = Duration.between(created, fired);
        System.out.printf("[AT-FLW-24.2] 파티션 %d 담당 %s를 대기 %dms째 kill -9, %s가 생성 %dms 뒤 발화(지속 %ds), 아웃박스 %d행, 발행 %d건%n",
                partition, victim.name(), Duration.between(created, killedAt).toMillis(), survivor.name(), elapsed.toMillis(),
                FOR.toSeconds(), outbox(key), commands(key).size());

        assertThat(outbox(key)).as("아웃박스 1행").isEqualTo(1);
        assertThat(commands(key)).as("action.commands 발행 1건").hasSize(1);
        assertThat(elapsed).as("처음부터 다시 세지 않음").isBetween(FOR, FOR.plusSeconds(5));
        assertThat(survivor.alive()).isTrue();
        c = FlowEngineProcess.start("flow-nfr-c", VHOST, core.baseUrl()).awaitReady(Duration.ofMinutes(3));
        if (victim == a) {
            a = null;
        } else {
            b = null;
        }
    }

    @Test
    @Order(2)
    @DisplayName("[FLW-05.02] TC-FLW-098 타이머를 잠근 인스턴스를 발화 트랜잭션 도중(아웃박스 INSERT 대기) kill -9 → 다른 인스턴스가 한 번만 발화, 아웃박스 1행·발행 1건")
    void killLockHolderMidCommit() throws Exception {
        CanonicalTelemetry t = hot(7702);
        String key = ActionIdempotencyKeys.flow(FLOW.toString(), "n-act00001", t.messageId().toString());
        List<FlowEngineProcess> alive = java.util.stream.Stream.of(a, b, c).filter(p -> p != null && p.alive()).toList();
        assertThat(alive).hasSize(2);

        try (Connection blocker = DriverManager.getConnection(TestInfrastructure.POSTGRES.getJdbcUrl(),
                TestInfrastructure.POSTGRES.getUsername(), TestInfrastructure.POSTGRES.getPassword())) {
            blocker.setAutoCommit(false);
            try (PreparedStatement ps = blocker.prepareStatement("""
                    INSERT INTO data2flow_flow.flow_outboxes (organization_id, idempotency_key, kind, routing_key, payload, flow_id,
                                                              flow_version, node_id, trigger_message_id)
                    VALUES (?, ?, 'COMMAND', 'command', '{}'::jsonb, ?, 1, 'n-act00001', 'blocker')""")) {
                ps.setLong(1, ORG);
                ps.setString(2, key);
                ps.setObject(3, FLOW);
                ps.executeUpdate();   // 커밋하지 않는다: 발화 트랜잭션의 같은 키 INSERT가 이 행을 기다린다
            }

            streams.publish(t);
            String holder = await().atMost(Duration.ofSeconds(40)).until(() -> jdbc.sql("""
                            SELECT application_name FROM pg_stat_activity
                             WHERE application_name LIKE 'flow-nfr-%' AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO flow_outboxes%'""")
                    .query(String.class).optional().orElse(null), java.util.Objects::nonNull);
            FlowEngineProcess victim = alive.stream().filter(p -> p.name().equals(holder)).findFirst().orElseThrow();
            FlowEngineProcess survivor = alive.stream().filter(p -> p != victim).findFirst().orElseThrow();
            assertThat(timer("device:7702").get("status")).as("잠긴 채 아직 발화 전").isEqualTo("WAITING");

            victim.kill();
            await().atMost(Duration.ofSeconds(20)).until(() -> jdbc.sql(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = :n").param("n", holder).query(Long.class).single() == 0);
            blocker.rollback();

            await().atMost(Duration.ofSeconds(30)).until(() -> "FIRED".equals(timer("device:7702").get("status")));
            await().atMost(Duration.ofSeconds(20)).until(() -> commands(key).size() == 1);
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6)).until(() -> commands(key).size() == 1 && outbox(key) == 1);
            System.out.printf("[TC-FLW-098] 발화 도중 %s kill -9 → %s가 발화, 아웃박스 %d행, 발행 %d건%n", victim.name(), survivor.name(),
                    outbox(key), commands(key).size());

            assertThat(outbox(key)).isEqualTo(1);
            assertThat(commands(key)).hasSize(1);
            assertThat(survivor.alive()).isTrue();
        }
    }
}
