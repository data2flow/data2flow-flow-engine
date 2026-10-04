package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.flow.FlowNode;
import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.sink.SinkMode;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import net.java21.data2flow.flow.plan.domain.ActionDraft;
import net.java21.data2flow.flow.plan.domain.CompileContext;
import net.java21.data2flow.flow.plan.domain.CompiledNode;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.NodeConfigException;
import net.java21.data2flow.flow.plan.domain.NodeContext;
import net.java21.data2flow.flow.plan.domain.NodeType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code sink.database}(FLW-04.02, TC-FLW-058, ADR-025): 레코드를 외부 저장소(PostgreSQL·MySQL·InfluxDB …)에 쓰라는 요청을 만든다. 엔진은
 * 저장소에 직접 쓰지 않고 {@link SinkWriteRequest}를 행동 요청(kind=SINK, 라우팅 키 {@code sink})으로 아웃박스에 쓰기만 한다. 실제 쓰기·
 * 재시도·dead-letter·스키마 확인은 action의 Sink 커넥터가 한다(BR-FLW-28). 저장소 종류가 바뀌어도(연결만 바꿈) 이 노드는 같다(FLW-04.02).
 *
 * <ul>
 *   <li>입력: {@code payload}가 배열이면 원소마다 레코드(앞단 JS 함수가 돌려준 레코드 배열), 객체면 레코드 하나.</li>
 *   <li>매핑 {@code mapping[{field, column}]}: {@code field}는 레코드 안 경로({@code temperature}), {@code $.}로 시작하면 메시지 경로
 *       ({@code $.deviceId}, {@code $.measuredAt}). 매핑이 없으면 레코드의 원시값 필드를 그대로 쓴다. 레코드 키는 대상 열 이름이다.</li>
 *   <li>배치: {@code batchSize}(기본 100, 최대 1,000)씩 요청 하나. 멱등 키는 배치가 하나면 {@code sha256(flowId, nodeId, 원인 메시지)}, 여럿이면
 *       분할 인덱스를 붙인다(BR-FLW-13).</li>
 *   <li>포트: 요청을 아웃박스에 쓰면 {@code ok}(원래 메시지 + {@code sink:{batches, records}}), 레코드를 만들 수 없으면(빈 입력·UPSERT 키 없음)
 *       {@code failed}(+ {@code sink.error}). 쓰기 결과는 action이 dead-letter로 다룬다.</li>
 * </ul>
 */
public class SinkDatabaseNodeType implements NodeType {

    public static final String TYPE = "sink.database";
    private final FlowNodeType descriptor = NodeDescriptors.load(TYPE);

    @Override
    public FlowNodeType descriptor() {
        return descriptor;
    }

    @Override
    public CompiledNode compile(FlowNode node, CompileContext context) {
        JsonNode config = node.config() == null ? Jsons.object() : node.config();
        Long connectionId = Jsons.id(config, "connectionId", "config.connectionId");
        if (connectionId == null) {
            throw new NodeConfigException("config.connectionId", "저장소 연결(connectionId)이 필요합니다");
        }
        String target = Jsons.text(config, "target");
        if (target == null || target.isBlank() || target.length() > 128) {
            throw new NodeConfigException("config.target", "대상 테이블·measurement(target)는 1~128자입니다");
        }
        String modeText = Jsons.text(config, "mode");
        SinkMode mode;
        try {
            mode = modeText == null ? SinkMode.INSERT : SinkMode.valueOf(modeText.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new NodeConfigException("config.mode", "쓰기 모드는 insert 또는 upsert입니다: " + modeText);
        }
        if (mode == SinkMode.UNKNOWN) {
            throw new NodeConfigException("config.mode", "쓰기 모드는 insert 또는 upsert입니다: " + modeText);
        }
        List<String> upsertKeys = new ArrayList<>();
        config.path("upsertKeys").values().forEach(v -> upsertKeys.add(v.asString()));
        if (mode == SinkMode.UPSERT && upsertKeys.isEmpty()) {
            throw new NodeConfigException("config.upsertKeys", "upsert에는 키 열(upsertKeys)이 1개 이상 필요합니다");
        }
        Map<String, String> mapping = new LinkedHashMap<>();
        JsonNode m = config.get("mapping");
        if (m != null && !m.isNull()) {
            if (!m.isArray()) {
                throw new NodeConfigException("config.mapping", "매핑은 [{field, column}] 배열입니다");
            }
            int i = 0;
            for (JsonNode item : m.values()) {
                String field = Jsons.text(item, "field");
                String column = Jsons.text(item, "column");
                if (field == null || field.isBlank() || column == null || column.isBlank()) {
                    throw new NodeConfigException("config.mapping[" + i + "]", "매핑에는 field와 column이 필요합니다");
                }
                mapping.put(column, field);
                i++;
            }
        }
        int batchSize = config.path("batchSize").asInt(SinkWriteRequest.DEFAULT_BATCH_SIZE);
        if (batchSize < 1 || batchSize > SinkWriteRequest.MAX_RECORDS) {
            throw new NodeConfigException("config.batchSize", "배치 크기는 1~" + SinkWriteRequest.MAX_RECORDS + "입니다");
        }
        return new Compiled(node.id(), context.flowId().toString(), context.organizationId(), connectionId, target, mode,
                List.copyOf(upsertKeys), java.util.Collections.unmodifiableMap(mapping), batchSize);
    }

    record Compiled(String nodeId, String flowId, long organizationId, long connectionId, String target, SinkMode mode,
                    List<String> upsertKeys, Map<String, String> mapping, int batchSize) implements CompiledNode {

        @Override
        public List<String> outputs() {
            return List.of("ok", "failed");
        }

        @Override
        public boolean isAction() {
            return true;
        }

        @Override
        public void onMessage(FlowMessage message, NodeContext ctx) {
            List<Map<String, Object>> collected = new ArrayList<>();
            JsonNode payload = message.body().path("payload");
            List<JsonNode> items = new ArrayList<>();
            if (payload.isArray()) {
                payload.values().forEach(items::add);
            } else if (payload.isObject()) {
                items.add(payload);
            }
            for (JsonNode item : items) {
                Map<String, Object> record = record(message, item);
                if (!record.isEmpty()) {
                    collected.add(record);
                }
            }
            List<Map<String, Object>> records = collected;
            if (mode == SinkMode.UPSERT) {
                records = dedupe(records);
            }
            List<SinkWriteRequest> batches;
            try {
                batches = SinkWriteRequest.batches(connectionId, target, mode, upsertKeys, records, batchSize);
            } catch (IllegalArgumentException e) {
                failed(message, ctx, e.getMessage());
                return;
            }
            if (batches.isEmpty()) {
                failed(message, ctx, "쓸 레코드가 없습니다(payload가 비었거나 원시값 필드가 없음)");
                return;
            }
            Clock fixed = Clock.fixed(ctx.now(), ZoneOffset.UTC);
            CommandSource source = CommandSource.flow(flowId, ctx.flowVersion(), nodeId, message.triggerMessageId());
            for (SinkWriteRequest batch : batches) {
                String key = batch.batchIndex() == null
                        ? ActionIdempotencyKeys.flow(flowId, nodeId, message.triggerMessageId())
                        : ActionIdempotencyKeys.flow(flowId, nodeId, message.triggerMessageId(), batch.batchIndex());
                ActionRequest request = ActionRequest.sink(organizationId, key, source, null, batch, fixed);
                ctx.action(new ActionDraft("SINK", MessagingNames.EXCHANGE_ACTIONS, request.routingKey(), key,
                        Jsons.MAPPER.valueToTree(request), "sink " + target + " ×" + batch.records().size()));
            }
            ObjectNode body = message.body().deepCopy();
            body.putObject("sink").put("batches", batches.size()).put("records", records.size());
            ctx.emit("ok", message.withBody(body));
        }

        /** UPSERT: 같은 키 레코드는 마지막 것 하나만(TC-FLW-058). 키 열이 빠진 레코드는 그대로 두어 요청 검증에서 실패시킨다 */
        private List<Map<String, Object>> dedupe(List<Map<String, Object>> records) {
            Map<List<Object>, Map<String, Object>> byKey = new LinkedHashMap<>();
            List<Map<String, Object>> keyless = new ArrayList<>();
            for (Map<String, Object> r : records) {
                List<Object> key = new ArrayList<>();
                boolean complete = true;
                for (String k : upsertKeys) {
                    complete &= r.containsKey(k);
                    key.add(r.get(k));
                }
                if (complete) {
                    byKey.remove(key);
                    byKey.put(key, r);
                } else {
                    keyless.add(r);
                }
            }
            List<Map<String, Object>> out = new ArrayList<>(byKey.values());
            out.addAll(keyless);
            return out;
        }

        private void failed(FlowMessage message, NodeContext ctx, String reason) {
            if (ctx.wired("failed")) {
                ObjectNode body = message.body().deepCopy();
                body.putObject("sink").put("error", reason);
                ctx.emit("failed", message.withBody(body));
            } else {
                ctx.fail(message, "SINK_RECORD_INVALID", reason);
            }
        }

        /** 레코드 하나: 매핑이 있으면 매핑한 열만, 없으면 원시값 필드 그대로. 값은 JSON 원시값 */
        private Map<String, Object> record(FlowMessage message, JsonNode item) {
            Map<String, Object> out = new LinkedHashMap<>();
            if (mapping.isEmpty()) {
                if (item.isObject()) {
                    item.properties().forEach(e -> {
                        Object v = primitive(e.getValue());
                        if (v != null) {
                            out.put(e.getKey(), v);
                        }
                    });
                }
                return out;
            }
            mapping.forEach((column, field) -> {
                JsonNode v = field.startsWith("$.") ? Jsons.at(message.body(), field.substring(2)) : Jsons.at(item, field);
                Object value = primitive(v);
                if (value != null) {
                    out.put(column, value);
                }
            });
            return out;
        }

        private static Object primitive(JsonNode v) {
            if (v == null || v.isNull() || v.isMissingNode()) {
                return null;
            }
            if (v.isBoolean()) {
                return v.booleanValue();
            }
            if (v.isIntegralNumber()) {
                return v.longValue();
            }
            if (v.isNumber()) {
                return v.doubleValue();
            }
            if (v.isString()) {
                return v.stringValue();
            }
            return v.toString();   // 객체·배열은 JSON 문자열로
        }
    }
}
