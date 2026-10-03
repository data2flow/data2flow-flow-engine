package net.java21.data2flow.flow.plan;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeType;
import net.java21.data2flow.flow.plan.service.NodeTypeRegistry;
import net.java21.data2flow.flow.support.FlowFixtures;
import net.java21.data2flow.flow.support.FlowTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TC-FLW-030 노드 종류 공통 계약(레지스트리의 모든 NodeType): ① 카탈로그 항목이 {@code flow-node-type.v1.json}을 통과하고 configSchema가
 * JSON Schema 2020-12 ② 대표 설정은 compile 성공, 필수 필드를 뺀 설정은 {@link NodeConfigException}(경로 포함) ③ 출력에 공통 error 포트(contracts
 * flow-node-type.v1.json이 모든 종류에 요구) ④ statePolicy가 모든 설정 필드에 KEEP·RESET·MIGRATE ⑥ 행동 노드는 FLOW_DEPLOY_CONTROL 권한과 재시도 기본값 3회.
 */
class NodeTypeContractTest {

    private static final NodeTypeRegistry REGISTRY = FlowTestHarness.registry(SpaceDirectory.NONE);
    private static final CompileContext CONTEXT = new CompileContext(FlowFixtures.FLOW, 1, 1);

    /** 종류별 대표 설정과 필수 필드 하나를 뺀 설정 */
    private static final Map<String, String[]> SAMPLES = Map.of(
            "trigger.telemetry", new String[]{"{\"target\":{\"spaceId\":\"31\"},\"metrics\":[\"temperature\"]}", "{}", "config.target"},
            "condition.threshold", new String[]{"{\"metric\":\"temperature\",\"op\":\">\",\"value\":27,\"for\":\"PT5M\",\"clear\":26}", "{\"op\":\">\"}", "config.value"},
            "condition.switch", new String[]{"{\"expression\":\"$.payload.mode\",\"cases\":[{\"name\":\"cool\",\"op\":\"==\",\"value\":\"cool\"}]}", "{\"cases\":[]}", "config.expression"},
            "transform.map", new String[]{"{\"rules\":[{\"op\":\"set\",\"path\":\"a\",\"value\":1}]}", "{}", "config.rules"},
            "transform.aggregate", new String[]{"{\"window\":\"PT5M\",\"fn\":\"avg\",\"groupBy\":\"space\"}", "{\"window\":\"PT5M\"}", "config.fn"},
            "transform.js", new String[]{"{\"code\":\"return msg;\",\"outputs\":1}", "{}", "config.code"},
            "flow.delay", new String[]{"{\"duration\":\"PT30S\"}", "{}", "config.duration"},
            "action.control", new String[]{"{\"target\":{\"spaceId\":31},\"capability\":\"Thermostat\",\"command\":\"set\",\"args\":{\"mode\":\"cool\"}}", "{\"target\":{\"spaceId\":31},\"command\":\"set\"}", "config.capability"},
            "debug.log", new String[]{"{\"level\":\"INFO\"}", "{\"level\":\"LOUD\"}", "config.level"});

    static Stream<NodeType> nodeTypes() {
        return REGISTRY.all().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nodeTypes")
    @DisplayName("[FLW-01.02] TC-FLW-030 카탈로그 항목은 flow-node-type.v1.json을 통과하고, 대표 설정은 configSchema(2020-12)를 통과한다")
    void descriptorIsValid(NodeType type) {
        FlowNodeType d = type.descriptor();
        assertThat(MessageSchemas.validate("flow-node-type.v1.json", Jsons.MAPPER.valueToTree(d))).as(type.type()).isEmpty();
        assertThat(SAMPLES).containsKey(type.type());
        assertThat(MessageSchemas.validateWithSchema(d.configSchema(), Jsons.MAPPER.readTree(SAMPLES.get(type.type())[0])))
                .as(type.type() + " configSchema").isEmpty();
        assertThat(d.typeVersion()).isEqualTo(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nodeTypes")
    @DisplayName("[FLW-01.02] TC-FLW-030 대표 설정은 컴파일되고 필수 필드가 빠지면 경로가 있는 NodeConfigException")
    void compileContract(NodeType type) {
        String[] sample = SAMPLES.get(type.type());
        FlowNode ok = new FlowNode("n-test0001", type.type(), 1, null, Jsons.MAPPER.readTree(sample[0]), null, null, null, null);
        FlowNode missing = new FlowNode("n-test0001", type.type(), 1, null, Jsons.MAPPER.readTree(sample[1]), null, null, null, null);

        assertThat(type.compile(ok, CONTEXT).outputs()).isNotEmpty();
        assertThatThrownBy(() -> type.compile(missing, CONTEXT)).isInstanceOf(NodeConfigException.class)
                .satisfies(e -> assertThat(((NodeConfigException) e).path()).isEqualTo(sample[2]));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nodeTypes")
    @DisplayName("[FLW-05.03] TC-FLW-030 트리거가 아니면 공통 error 포트, statePolicy는 모든 설정 필드에 KEEP·RESET·MIGRATE")
    void portsAndStatePolicy(NodeType type) {
        FlowNodeType d = type.descriptor();
        assertThat(d.outputs()).anySatisfy(p -> assertThat(p.name()).isEqualTo(FlowNodeType.ERROR_PORT));
        assertThat(d.inputs()).as("트리거만 입력이 없다").hasSize("trigger".equals(d.category()) ? 0 : 1);
        JsonNode props = d.configSchema().path("properties");
        for (String field : props.propertyNames()) {
            assertThat(d.statePolicy().path(field).asString("")).as(type.type() + "." + field).isIn("KEEP", "RESET", "MIGRATE");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nodeTypes")
    @DisplayName("[FLW-05.06] TC-FLW-030 행동 노드는 FLOW_DEPLOY_CONTROL 권한과 재시도 기본 3회(BR-FLW-21)")
    void actionNodes(NodeType type) {
        FlowNodeType d = type.descriptor();
        if ("action".equals(d.category())) {
            assertThat(d.permissions()).contains("FLOW_DEPLOY_CONTROL");
            assertThat(d.defaults().path("retry").path("maxAttempts").asInt()).isEqualTo(3);
        } else {
            assertThat(d.defaults().path("retry").path("maxAttempts").asInt()).isZero();
        }
    }
}
