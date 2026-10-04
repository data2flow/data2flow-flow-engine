package net.java21.data2flow.flow.nfr;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.flow.plan.domain.LoadedFlow;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.runtime.domain.ExecutionListener;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.IntegrationTestSupport;
import net.java21.data2flow.flow.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 라이브 리로드 부하 시험(FLW test-plan TC-FLW-134, M4 완료 확인 "초당 200건 중 플로우 10회 연속 배포에서 모든 메시지가 정확히 한 버전으로
 * 처리됨", NFR "감지 → 제어 p95 2초"). 실제 엔진(스트림 소비·설정 fanout·아웃박스 릴레이)에 초당 200건을 60초 넣으면서 5초마다 새 버전을
 * 적용한다(v12 → v13 → … → v22, core 대역을 바꾸고 {@code data2flow.config}에 FLOW 변경을 낸다).
 *
 * <ul>
 *   <li>처리 기록: 커밋된 실행마다 (메시지, 처리 버전)을 모은다({@link ExecutionListener}, 되돌린 실행은 오지 않는다).</li>
 *   <li>모든 메시지가 제어 명령 하나를 낸다(온도 30 &gt; 기준 27·27.1). 감지 → 제어는 텔레메트리 수신 시각부터 아웃박스 행이 publisher confirm을
 *       받아 {@code action.commands}에 들어간 시각({@code sent_at})까지다.</li>
 * </ul>
 */
class LiveReloadIT extends IntegrationTestSupport {

    private static final Clock WALL = Clock.systemUTC();
    private static final MessageCodec CODEC = MessageCodec.create();
    private static final int RATE_PER_SEC = 200;
    private static final int SECONDS = 60;
    private static final int TOTAL = RATE_PER_SEC * SECONDS;
    private static final int DEPLOYS = 10;
    private static final int FIRST_VERSION = 12;

    @Autowired
    FlowRegistry registry;
    @Autowired
    FlowRuntimeService runtime;
    @Autowired
    JdbcClient jdbc;

    /** 버전마다 기준값만 다른 정의(상태 정책 KEEP 필드) */
    private static FlowDefinition definition(String model, int version) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"modelId":"%s"},"metrics":["temperature"]}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":%s}},
                  {"id":"n-act00001","type":"action.control","config":{"target":{"deviceId":"7700"},"capability":"Switch","command":"set","args":{"on":true}}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"},{"from":"n-thr00001","port":"true","to":"n-act00001"}]}"""
                .formatted(model, version % 2 == 0 ? "27" : "27.1"));
    }

    private void deploy(UUID flow, String model, int version) {
        // 초당 200건을 받으므로 초당 실행 한도를 최대(1,000)로 둔다(기본 100이면 FLW-05.04 안전장치가 멈춘다)
        TestInfrastructure.CORE.put(flow, FlowFixtures.ORG, version, "ACTIVE", definition(model, version), List.of(), List.of(), 0,
                1000, "DROP");
        STREAMS.publishConfig(CODEC.write(new ConfigChangedMessage(ConfigChangedMessage.VERSION, UUID.randomUUID(),
                ConfigChangedMessage.EntityType.FLOW, flow.toString(), version, ConfigChangedMessage.Op.UPSERT,
                Long.toString(FlowFixtures.ORG), Instant.now(WALL))));
    }

    /**
     * 예열: 운영 엔진은 계속 돌고 있어 JIT·연결 풀·스트림 소비가 데워져 있다. 측정 전에 다른 플로우로 초당 200건을 5초 넣고 처리가 끝날 때까지
     * 기다린다(측정에는 넣지 않음).
     */
    private void warmUp() {
        UUID flow = UUID.randomUUID();
        String model = "warm-" + flow.toString().substring(0, 8);
        deploy(flow, model, 1);
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).isPresent());
        List<CompletableFuture<Boolean>> confirms = new ArrayList<>();
        for (int n = 0; n < RATE_PER_SEC * 5; n++) {
            Instant now = Instant.now(WALL);
            confirms.add(STREAMS.publishAsync(CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3)
                    .externalId("warm-" + (n % 200)).deviceId(80_000 + (n % 200)).modelId(model).measuredAt(now).receivedAt(now)
                    .metric(CanonicalTelemetry.Metric.of("temperature", 30, "℃")).rawMessageId(n + 1).build()));
        }
        confirms.forEach(CompletableFuture::join);
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(500)).until(() -> jdbc.sql(
                        "SELECT count(*) FROM data2flow_flow.flow_outboxes WHERE flow_id = :f AND sent_at IS NOT NULL")
                .param("f", flow).query(Long.class).single() >= RATE_PER_SEC * 5);
    }

    @Test
    @DisplayName("[FLW-06.02][FLW-06.01][AT-FLW-03.1] TC-FLW-134 초당 200건 60초 부하 중 v12→v22를 5초 간격으로 10회 교체: 메시지마다 처리 버전 1개, "
            + "입력 12,000 = 처리 12,000(유실 0·중복 0), 감지→제어 p95 ≤ 2초")
    void everyMessageIsProcessedByExactlyOneVersion() throws Exception {
        warmUp();
        UUID flow = UUID.randomUUID();
        String model = "reload-" + flow.toString().substring(0, 8);
        deploy(flow, model, FIRST_VERSION);
        await().atMost(Duration.ofSeconds(20)).until(() -> registry.get(flow).map(f -> f.version() == FIRST_VERSION).orElse(false));

        Map<String, Set<Integer>> versionsByMessage = new ConcurrentHashMap<>();
        AtomicInteger executions = new AtomicInteger();
        ExecutionListener recorder = new ExecutionListener() {
            @Override
            public void executed(LoadedFlow f, ExecutionReport report) {
                if (f.flowId().equals(flow)) {
                    executions.incrementAndGet();
                    versionsByMessage.computeIfAbsent(report.triggerMessageId(), k -> ConcurrentHashMap.newKeySet())
                            .add(report.version());
                }
            }
        };
        runtime.addListener(recorder);
        Map<String, Instant> receivedAt = new ConcurrentHashMap<>();
        List<CompletableFuture<Boolean>> confirms = java.util.Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService load = Executors.newScheduledThreadPool(2);
        try {
            // 부하: 100ms마다 20건(초당 200건), 기기 200대에 고르게(파티션 3개에 나뉨)
            AtomicInteger sequence = new AtomicInteger();
            int perTick = RATE_PER_SEC / 10;
            var generator = load.scheduleAtFixedRate(() -> {
                for (int i = 0; i < perTick; i++) {
                    int n = sequence.getAndIncrement();
                    if (n >= TOTAL) {
                        return;
                    }
                    Instant now = Instant.now(WALL);
                    CanonicalTelemetry t = CanonicalTelemetry.builder().organizationId(FlowFixtures.ORG).sourceId(3)
                            .externalId("reload-" + (n % 200)).deviceId(70_000 + (n % 200)).modelId(model)
                            .measuredAt(now).receivedAt(now).metric(CanonicalTelemetry.Metric.of("temperature", 30, "℃"))
                            .rawMessageId(n + 1).build();
                    receivedAt.put(t.messageId().toString(), now);
                    confirms.add(STREAMS.publishAsync(t));
                }
            }, 0, 100, TimeUnit.MILLISECONDS);
            // 배포: 5초마다 새 버전(v13~v22)
            for (int k = 1; k <= DEPLOYS; k++) {
                int version = FIRST_VERSION + k;
                load.schedule(() -> deploy(flow, model, version), 5L * k, TimeUnit.SECONDS);
            }

            await().atMost(Duration.ofSeconds(SECONDS + 30)).pollInterval(Duration.ofMillis(500))
                    .until(() -> sequence.get() >= TOTAL);
            generator.cancel(false);
            for (CompletableFuture<Boolean> c : List.copyOf(confirms)) {
                assertThat(c.get(30, TimeUnit.SECONDS)).as("텔레메트리 발행 확인").isTrue();
            }
            assertThat(receivedAt).hasSize(TOTAL);

            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500))
                    .until(() -> versionsByMessage.size() >= TOTAL);
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).until(() -> jdbc.sql(
                            "SELECT count(*) FROM data2flow_flow.flow_outboxes WHERE flow_id = :f AND sent_at IS NOT NULL")
                    .param("f", flow).query(Long.class).single() >= TOTAL);
        } finally {
            load.shutdownNow();
            runtime.removeListener(recorder);
        }

        // 1) 메시지마다 처리 버전 1개(BR-FLW-06), 유실 0·중복 0
        assertThat(versionsByMessage.keySet()).as("유실 0: 입력한 모든 메시지가 처리됨").containsAll(receivedAt.keySet());
        assertThat(versionsByMessage.keySet()).as("입력하지 않은 메시지는 없음").hasSize(TOTAL);
        assertThat(versionsByMessage.values()).as("메시지마다 처리 버전 수 = 1").allSatisfy(v -> assertThat(v).hasSize(1));
        assertThat(executions.get()).as("중복 처리 0: 커밋된 실행 수 = 입력 수").isEqualTo(TOTAL);
        Map<Integer, Integer> perVersion = new TreeMap<>();
        versionsByMessage.values().forEach(v -> perVersion.merge(v.iterator().next(), 1, Integer::sum));
        assertThat(perVersion.keySet()).as("교체가 부하 중에 일어남(여러 버전이 메시지를 처리함)").hasSizeGreaterThanOrEqualTo(DEPLOYS);
        assertThat(registry.get(flow).orElseThrow().version()).isEqualTo(FIRST_VERSION + DEPLOYS);
        await().atMost(Duration.ofSeconds(10)).until(() -> registry.sweep() == 0);

        // 2) 아웃박스: 메시지당 명령 1건, 버전 열 = 처리 버전
        List<Object[]> rows = jdbc.sql("""
                        SELECT trim(trigger_message_id), flow_version, sent_at, created_at FROM data2flow_flow.flow_outboxes WHERE flow_id = :f""")
                .param("f", flow).query((rs, n) -> new Object[]{rs.getString(1), rs.getInt(2), rs.getTimestamp(3).toInstant(),
                        rs.getTimestamp(4).toInstant()}).list();
        assertThat(rows).hasSize(TOTAL);
        List<Long> latencies = new ArrayList<>();
        List<Long> processing = new ArrayList<>();
        for (Object[] row : rows) {
            assertThat(versionsByMessage.get((String) row[0])).containsExactly((Integer) row[1]);
            latencies.add(Duration.between(receivedAt.get((String) row[0]), (Instant) row[2]).toMillis());
            processing.add(Duration.between(receivedAt.get((String) row[0]), (Instant) row[3]).toMillis());
        }
        processing.sort(Long::compare);
        // 진단: 5초 구간별 감지→제어 최대(배포 시점과 지연이 겹치는지 본다)
        Instant start = receivedAt.values().stream().min(Instant::compareTo).orElseThrow();
        Map<Long, Long> worstPerWindow = new TreeMap<>();
        for (Object[] row : rows) {
            long window = Duration.between(start, receivedAt.get((String) row[0])).toSeconds() / 5 * 5;
            worstPerWindow.merge(window, Duration.between(receivedAt.get((String) row[0]), (Instant) row[2]).toMillis(), Math::max);
        }
        System.out.println("[TC-FLW-134] 5초 구간별 최대 지연(ms): " + worstPerWindow);
        latencies.sort(Long::compare);
        long p50 = latencies.get(latencies.size() / 2);
        long p95 = latencies.get((int) Math.ceil(latencies.size() * 0.95) - 1);
        long p99 = latencies.get((int) Math.ceil(latencies.size() * 0.99) - 1);
        System.out.printf("[TC-FLW-134] %d건/%d초, 배포 %d회, 버전별 처리 %s, 감지→아웃박스 기록 p95 %dms, "
                        + "감지→제어(publisher confirm) p50 %dms p95 %dms p99 %dms 최대 %dms%n", TOTAL, SECONDS, DEPLOYS, perVersion,
                processing.get((int) Math.ceil(processing.size() * 0.95) - 1), p50, p95, p99, latencies.getLast());
        assertThat(p95).as("감지→제어 p95 ≤ 2초(NFR)").isLessThanOrEqualTo(2000);

        // 3) action.commands에 명령 12,000건이 모두 들어감
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).until(() -> STREAMS.commands(flow).size() >= TOTAL);
        assertThat(STREAMS.commands(flow).stream().map(c -> c.idempotencyKey()).distinct().count()).isEqualTo(TOTAL);
    }
}
