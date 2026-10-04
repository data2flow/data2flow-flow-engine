package net.java21.data2flow.flow.nfr;

import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.FlowApplyReported;
import net.java21.data2flow.flow.support.CoreApiStub;
import net.java21.data2flow.flow.support.FlowEngineProcess;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.TestInfrastructure;
import net.java21.data2flow.flow.support.TestStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 다중 인스턴스 라이브 리로드(TC-FLW-136, design §4.2·§4 ⑦): 엔진 인스턴스 두 개(테스트 클래스패스로 띄운 별도 JVM)가 같은
 * {@code data2flow.config} 변경을 각자 받아 적용하고 `flow.apply.reported`를 낸다. 정의가 틀리면 두 인스턴스 모두 이전 버전을 유지하고 오류를
 * 보고한다(core는 모든 인스턴스가 같은 버전을 보고해야 "적용 완료").
 */
class LiveReloadMultiInstanceIT {

    private static final String VHOST = "flow-mi";
    private static final long ORG = 1;
    private static final UUID FLOW = UUID.randomUUID();
    private static final Clock WALL = Clock.systemUTC();
    private static final MessageCodec CODEC = MessageCodec.create();

    private static CoreApiStub core;
    private static TestStreams streams;
    private static JdbcClient jdbc;
    private static FlowEngineProcess a;
    private static FlowEngineProcess b;

    private static FlowDefinition definition(double value) {
        return FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["8801"]},"metrics":["temperature"]}},
                  {"id":"n-thr00001","type":"condition.threshold","config":{"metric":"temperature","op":">","value":%s,"for":"PT1M"}}],
                 "wires":[{"from":"n-trg00001","to":"n-thr00001"}]}""".formatted(value));
    }

    @BeforeAll
    static void startInstances() {
        TestInfrastructure.createVhost(VHOST);
        jdbc = JdbcClient.create(new DriverManagerDataSource(TestInfrastructure.POSTGRES.getJdbcUrl(),
                TestInfrastructure.POSTGRES.getUsername(), TestInfrastructure.POSTGRES.getPassword()));
        core = new CoreApiStub().organization(ORG);
        core.put(FLOW, ORG, 12, "ACTIVE", definition(27));
        streams = new TestStreams(VHOST, 3);
        streams.bindEvents("mi.apply", "flow.apply.reported");
        a = FlowEngineProcess.start("flow-mi-a", VHOST, core.baseUrl());
        b = FlowEngineProcess.start("flow-mi-b", VHOST, core.baseUrl());
        a.awaitReady(Duration.ofMinutes(3));
        b.awaitReady(Duration.ofMinutes(3));
        await().atMost(Duration.ofSeconds(30)).until(() -> applied(12) == 2);
    }

    @AfterAll
    static void stopInstances() {
        for (FlowEngineProcess p : new FlowEngineProcess[]{a, b}) {
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

    private static long applied(int version) {
        return jdbc.sql("SELECT count(*) FROM data2flow_flow.flow_instance_versions WHERE flow_id = :f AND applied_version = :v")
                .param("f", FLOW).param("v", version).query(Long.class).single();
    }

    private static void publishFlowChange(int version) {
        streams.publishConfig(CODEC.write(new ConfigChangedMessage(ConfigChangedMessage.VERSION, UUID.randomUUID(),
                ConfigChangedMessage.EntityType.FLOW, FLOW.toString(), version, ConfigChangedMessage.Op.UPSERT, Long.toString(ORG),
                Instant.now(WALL))));
    }

    private static List<FlowApplyReported> reports() {
        List<FlowApplyReported> out = new ArrayList<>();
        for (byte[] body : streams.drain("mi.apply")) {
            DomainEvent<?> e = CODEC.readEvent(body);
            if (e.payload() instanceof FlowApplyReported r && r.flowId().equals(FLOW.toString())) {
                out.add(r);
            }
        }
        return out;
    }

    @Test
    @DisplayName("[FLW-06.01][FLW-06.02] TC-FLW-136 인스턴스 2개가 config 변경을 받아 둘 다 v13 적용·보고, 틀린 v14는 둘 다 컴파일 실패를 보고하고 v13 유지")
    void bothInstancesConvergeAndKeepVersionOnFailure() {
        reports();
        core.put(FLOW, ORG, 13, "ACTIVE", definition(28));
        publishFlowChange(13);

        List<FlowApplyReported> v13 = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            v13.addAll(reports().stream().filter(r -> r.appliedVersion() == 13 && r.error() == null).toList());
            return v13.stream().map(FlowApplyReported::instanceId).collect(Collectors.toSet()).size() == 2;
        });
        assertThat(v13.stream().map(FlowApplyReported::instanceId).collect(Collectors.toSet())).containsExactlyInAnyOrder("flow-mi-a",
                "flow-mi-b");
        assertThat(applied(13)).isEqualTo(2);

        core.put(FLOW, ORG, 14, "ACTIVE", FlowFixtures.definition("""
                {"schema":"data2flow.flow-definition/v1","nodes":[
                  {"id":"n-trg00001","type":"trigger.telemetry","config":{"target":{"deviceIds":["8801"]}}},
                  {"id":"n-bad00001","type":"no.such.node","config":{}}],
                 "wires":[{"from":"n-trg00001","to":"n-bad00001"}]}"""));
        publishFlowChange(14);
        List<FlowApplyReported> failed = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            failed.addAll(reports().stream().filter(r -> r.error() != null).toList());
            return failed.stream().map(FlowApplyReported::instanceId).collect(Collectors.toSet()).size() == 2;
        });
        assertThat(failed).allSatisfy(r -> {
            assertThat(r.appliedVersion()).as("이전 버전 유지").isEqualTo(13);
            assertThat(r.error()).contains("UNKNOWN_NODE_TYPE");
        });
        assertThat(applied(13)).isEqualTo(2);
        assertThat(applied(14)).isZero();
        assertThat(Map.of("a", a.alive(), "b", b.alive())).containsValues(true, true);
    }
}
