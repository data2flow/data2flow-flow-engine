package net.java21.data2flow.flow.plan.domain;

import net.java21.data2flow.contracts.message.MessageCodec;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/** 노드 설정·메시지 JSON 도우미(Jackson 3) */
public final class Jsons {

    public static final JsonMapper MAPPER = MessageCodec.newMapper();
    public static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private Jsons() {
    }

    public static ObjectNode object() {
        return NODES.objectNode();
    }

    /** 문자열 값(숫자면 문자열로). 없거나 null이면 null */
    public static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        if (v == null || v.isNull() || v.isMissingNode()) {
            return null;
        }
        if (v.isString()) {
            return v.stringValue();
        }
        if (v.isNumber() || v.isBoolean()) {
            return v.toString();
        }
        return null;
    }

    /** 숫자 값. 숫자 모양 문자열("31")도 받는다. 없으면 null */
    public static Double number(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return number(v);
    }

    public static Double number(JsonNode v) {
        if (v == null || v.isNull() || v.isMissingNode()) {
            return null;
        }
        if (v.isNumber()) {
            return v.doubleValue();
        }
        if (v.isString()) {
            try {
                return Double.parseDouble(v.stringValue().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /** ID 값(숫자 또는 숫자 문자열, api-rules: ID는 JSON 문자열). 없으면 null, 형식이 틀리면 예외 */
    public static Long id(JsonNode node, String field, String path) {
        JsonNode v = node == null ? null : node.get(field);
        if (v == null || v.isNull() || v.isMissingNode()) {
            return null;
        }
        Double d = number(v);
        if (d == null || d != Math.floor(d) || d < 1) {
            throw new NodeConfigException(path, "ID는 1 이상의 정수(또는 정수 문자열)여야 합니다: " + v);
        }
        return d.longValue();
    }

    /** 점 경로(예: {@code payload.temperature}, {@code metrics[0].value}) 값. 없으면 MissingNode */
    public static JsonNode at(JsonNode root, String path) {
        JsonNode current = root;
        for (String segment : segments(path)) {
            if (current == null) {
                return missing();
            }
            if (segment.startsWith("[")) {
                int index = Integer.parseInt(segment.substring(1, segment.length() - 1));
                current = current.isArray() && index < current.size() ? current.get(index) : null;
            } else {
                current = current.isObject() ? current.get(segment) : null;
            }
        }
        return current == null ? missing() : current;
    }

    /** JSONPath 부분집합({@code $.payload.mode}, {@code $['payload'].mode}, {@code payload.mode})을 점 경로로 */
    public static String normalizePath(String expression) {
        String p = expression.trim();
        if (p.startsWith("$")) {
            p = p.substring(1);
        }
        p = p.replaceAll("\\['([^']*)'\\]", ".$1").replaceAll("\\[\"([^\"]*)\"\\]", ".$1");
        if (p.startsWith(".")) {
            p = p.substring(1);
        }
        return p;
    }

    static java.util.List<String> segments(String path) {
        java.util.List<String> out = new java.util.ArrayList<>();
        for (String part : path.split("\\.")) {
            if (part.isEmpty()) {
                continue;
            }
            int bracket = part.indexOf('[');
            if (bracket < 0) {
                out.add(part);
                continue;
            }
            if (bracket > 0) {
                out.add(part.substring(0, bracket));
            }
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\[(\\d+)]").matcher(part.substring(bracket));
            while (m.find()) {
                out.add("[" + m.group(1) + "]");
            }
        }
        return out;
    }

    private static JsonNode missing() {
        return tools.jackson.databind.node.MissingNode.getInstance();
    }
}
