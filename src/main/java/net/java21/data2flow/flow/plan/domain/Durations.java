package net.java21.data2flow.flow.plan.domain;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 노드 설정의 기간 값: ISO-8601({@code PT5M}, 편집기 기본) 또는 짧은 표기({@code 5m}, {@code 30s}, {@code 1h}, {@code 500ms}) */
public final class Durations {

    private static final Pattern SHORT = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h|d)");

    private Durations() {
    }

    /** 해석할 수 없으면 {@link NodeConfigException} */
    public static Duration parse(String value, String path) {
        if (value == null || value.isBlank()) {
            throw new NodeConfigException(path, "기간이 비어 있습니다");
        }
        String v = value.trim();
        try {
            if (v.startsWith("P") || v.startsWith("p")) {
                return Duration.parse(v.toUpperCase());
            }
        } catch (DateTimeParseException e) {
            throw new NodeConfigException(path, "기간 형식이 아닙니다(예: PT5M, 5m): " + value);
        }
        Matcher m = SHORT.matcher(v);
        if (!m.matches()) {
            throw new NodeConfigException(path, "기간 형식이 아닙니다(예: PT5M, 5m): " + value);
        }
        double amount = Double.parseDouble(m.group(1));
        long millis = switch (m.group(2)) {
            case "ms" -> (long) amount;
            case "s" -> (long) (amount * 1_000);
            case "m" -> (long) (amount * 60_000);
            case "h" -> (long) (amount * 3_600_000);
            default -> (long) (amount * 86_400_000);
        };
        return Duration.ofMillis(millis);
    }
}
