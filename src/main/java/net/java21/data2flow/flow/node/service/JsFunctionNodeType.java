package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import net.java21.data2flow.script.sandbox.ScriptFailure;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code transform.js}(FLW-02 JavaScript 함수, TC-FLW-046, ADR-008). GraalJS 커뮤니티판 샌드박스({@link ScriptSandbox}, 공용 모듈
 * data2flow-script-sandbox(ADR-046), SCR와 같은 제한: CPU 50ms·문장 100만·출력 64KB·호스트 접근 금지)에서 실행한다. 샌드박스는
 * ctx 추가 필드 {@link #CONTEXT_KEYS}(flow·node)로 만들어야 한다({@code FlowEngineConfig}).
 *
 * <p>코드 계약(FLW-api §5.2): 코드는 {@code function (msg, ctx)}의 <b>본문</b>이다({@code return {...msg, value: msg.payload.t / 10};}).
 * 본문 안에 {@code function main(msg, ctx)}를 정의하고 아무것도 반환하지 않으면 엔진이 {@code main(msg, ctx)}를 부른다.
 * {@code ctx}는 읽기 전용 {@code {flow:{id, version}, node:{id, name}, util, log}}.
 * <ul>
 *   <li>반환 객체 → {@code out1}. 반환 값이 객체가 아니면(숫자 등) 입력 메시지의 {@code payload}를 그 값으로 바꾼 메시지</li>
 *   <li>배열 → 길이가 출력 수와 같아야 하고 i번째가 {@code out(i+1)}로(null은 건너뜀, 배열 원소가 배열이면 여러 건). 길이가 다르면 error</li>
 *   <li>null·undefined → 아무것도 내보내지 않음</li>
 *   <li>예외·시간 초과·금지 API → error 포트 {@code SCRIPT_ERROR}(상세 코드 SCRIPT_TIMEOUT 등을 메시지에)</li>
 * </ul>
 * {@code scriptRef}({@code s-{id}@v{n}}, M4): {@code code} 대신 SCR 스크립트의 활성 버전 코드를 컴파일할 때 읽어 쓴다(core API-SCR-32).
 * 코드 계약은 같다(함수 본문 또는 {@code main(msg, ctx)}).
 */
public class JsFunctionNodeType implements NodeType {

    public static final String TYPE = "transform.js";
    static final String ENTRY = "__d2f_flow";
    /** 샌드박스가 ctx에 더할 읽기 전용 필드(공용 샌드박스의 기본 ctx 필드 외) */
    public static final List<String> CONTEXT_KEYS = List.of("flow", "node");
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);
    private static final java.util.regex.Pattern SCRIPT_REF = java.util.regex.Pattern.compile("^s-(\\d+)@v(\\d+)$");
    private final ScriptSandbox sandbox;
    private final ScriptDirectory scripts;

    public JsFunctionNodeType(ScriptSandbox sandbox) {
        this(sandbox, ScriptDirectory.NONE);
    }

    public JsFunctionNodeType(ScriptSandbox sandbox, ScriptDirectory scripts) {
        this.sandbox = sandbox;
        this.scripts = scripts;
    }

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        String code = Jsons.text(config, "code");
        if (config.hasNonNull("scriptRef") && (code == null || code.isBlank())) {
            code = resolve(Jsons.text(config, "scriptRef"), context.organizationId());
        }
        if (code == null || code.isBlank()) {
            throw new NodeConfigException("config.code", "코드가 필요합니다");
        }
        if (code.getBytes(StandardCharsets.UTF_8).length > sandbox.limits().maxCodeBytes()) {
            throw new NodeConfigException("config.code", "LIMIT", "JS 노드 코드는 64KB 이하입니다(BR-FLW-16)");
        }
        JsonNode o = config.get("outputs");
        int outputs = o == null || o.isNull() ? 1 : o.asInt(0);
        if (outputs < 1 || outputs > 10) {
            throw new NodeConfigException("config.outputs", "출력 수는 1~10입니다");
        }
        String wrapped = "function " + ENTRY + "(msg, ctx) {\n" + code
                + "\n;if (typeof main === 'function') { return main(msg, ctx); }\n}";
        ScriptFailure syntax = sandbox.syntaxCheck(wrapped, sourceName(context, node));
        if (syntax != null) {
            Integer line = syntax.line() == null ? null : Math.max(1, syntax.line() - 1);
            throw new NodeConfigException("config.code", "문법 오류" + (line == null ? "" : "(" + line + "번 줄)") + ": "
                    + syntax.message());
        }
        List<String> ports = new ArrayList<>();
        for (int i = 1; i <= outputs; i++) {
            ports.add("out" + i);
        }
        ObjectNode ctx = Jsons.object();
        ctx.putObject("flow").put("id", context.flowId().toString()).put("version", context.version());
        ctx.putObject("node").put("id", node.id()).put("name", node.name() == null ? node.id() : node.name());
        return new Compiled(sandbox, wrapped, sourceName(context, node), List.copyOf(ports), ctx.toString());
    }

    /** {@code s-{id}@v{n}} → 코드(SCR 활성 버전). 형식이 틀리거나 찾지 못하면 {@link NodeConfigException} */
    private String resolve(String ref, long organizationId) {
        java.util.regex.Matcher m = SCRIPT_REF.matcher(ref == null ? "" : ref.trim());
        if (!m.matches()) {
            throw new NodeConfigException("config.scriptRef", "스크립트 참조는 s-{id}@v{n} 형식입니다: " + ref);
        }
        try {
            return scripts.code(organizationId, Long.parseLong(m.group(1)), Integer.parseInt(m.group(2)))
                    .orElseThrow(() -> new NodeConfigException("config.scriptRef", "SCRIPT_NOT_FOUND",
                            "스크립트 " + ref + "를 찾지 못했습니다(없거나 그 버전이 활성 버전이 아님)"));
        } catch (NodeConfigException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new NodeConfigException("config.scriptRef", "스크립트 " + ref + "를 읽지 못했습니다: " + e.getMessage());
        }
    }

    private static String sourceName(CompileContext context, FlowNode node) {
        return "flow-" + context.flowId() + "-v" + context.version() + "-" + node.id() + ".js";
    }

    record Compiled(ScriptSandbox sandbox, String code, String sourceName, List<String> outputs, String ctxJson)
            implements CompiledNode {

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            ScriptOutcome outcome = sandbox.run(ENTRY, code, sourceName, message.body().toString(), ctxJson, ctx.now());
            if (!outcome.ok()) {
                ScriptFailure f = outcome.failure();
                String where = f.line() == null ? "" : " (" + Math.max(1, f.line() - 1) + "번 줄)";
                ctx.fail(message, "SCRIPT_ERROR", f.code() + where + ": " + f.message());
                return;
            }
            JsonNode out = outcome.output();
            if (out == null || out.isNull() || out.isMissingNode()) {
                return;
            }
            if (out.isArray()) {
                if (out.size() != outputs.size()) {
                    ctx.fail(message, "SCRIPT_ERROR", "SCRIPT_OUTPUT_INVALID: 배열 길이(" + out.size() + ")가 출력 수("
                            + outputs.size() + ")와 다릅니다");
                    return;
                }
                for (int i = 0; i < out.size(); i++) {
                    JsonNode item = out.get(i);
                    if (item == null || item.isNull()) {
                        continue;
                    }
                    if (item.isArray()) {
                        for (JsonNode each : item.values()) {
                            if (!each.isNull()) {
                                ctx.emit(outputs.get(i), toMessage(message, each));
                            }
                        }
                    } else {
                        ctx.emit(outputs.get(i), toMessage(message, item));
                    }
                }
                return;
            }
            ctx.emit(outputs.getFirst(), toMessage(message, out));
        }

        private static FlowMessage toMessage(FlowMessage input, JsonNode value) {
            ObjectNode body;
            if (value.isObject()) {
                body = (ObjectNode) value.deepCopy();
                if (!body.has("messageId") && input.body().has("messageId")) {
                    body.set("messageId", input.body().get("messageId"));
                }
            } else {
                body = input.body().deepCopy();
                body.set("payload", value);
            }
            return input.withBody(body);
        }
    }
}
