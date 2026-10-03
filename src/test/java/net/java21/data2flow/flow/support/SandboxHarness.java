package net.java21.data2flow.flow.support;

import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.script.domain.ScriptOutcome;
import net.java21.data2flow.flow.script.service.ScriptSandbox;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

/**
 * 샌드박스 하네스(data2flow-pipeline의 ScriptSandboxHarness와 같은 것): 운영과 같은 {@link ScriptSandbox}를 운영 설정 파일
 * (application.yml {@code data2flow.flow.script})의 한도로 만든다. 공격 코퍼스는 {@code src/test/resources/script-attacks/*.js}
 * 파일 하나가 공격 하나다(첫 줄 {@code // expect: 코드|코드}). pipeline 저장소의 코퍼스 42종을 그대로 옮겨 왔다.
 */
public final class SandboxHarness {

    public static final String NORMAL_INPUT = "{\"v\":1,\"deviceId\":17,\"measuredAt\":\"2026-10-03T00:00:00Z\","
            + "\"metrics\":[{\"key\":\"temperature\",\"value\":22.04,\"unit\":\"℃\",\"quality\":0}]}";

    private static volatile ScriptSandbox sandbox;

    private SandboxHarness() {
    }

    public static synchronized ScriptSandbox sandbox() {
        if (sandbox == null) {
            FlowEngineProperties.Script script = properties().script();
            sandbox = new ScriptSandbox(script.toLimits());
            sandbox.warmUp(script.warmUpRounds());
        }
        return sandbox;
    }

    /** application.yml의 data2flow.flow.* 를 그대로 읽는다 */
    public static FlowEngineProperties properties() {
        try {
            List<PropertySource<?>> sources = new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
            Binder binder = new Binder(ConfigurationPropertySources.from(sources), new PropertySourcesPlaceholdersResolver(sources));
            return binder.bindOrCreate("data2flow.flow", FlowEngineProperties.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 공격 코퍼스는 pipeline 형식({@code function transform(msg, ctx)})이라 진입 함수 transform으로 실행한다 */
    public static ScriptOutcome transform(String code) {
        return sandbox().run("transform", code, "script.js", NORMAL_INPUT, "{}", Instant.parse("2026-10-03T00:00:00Z"));
    }

    public static ScriptOutcome transform(String code, String input, String ctx) {
        return sandbox().run("transform", code, "script.js", input, ctx, Instant.parse("2026-10-03T00:00:00Z"));
    }

    public static Stream<Attack> attacks(String prefix) {
        try {
            Path dir = Path.of(SandboxHarness.class.getResource("/script-attacks").toURI());
            try (Stream<Path> files = Files.list(dir)) {
                return files.filter(p -> p.getFileName().toString().startsWith(prefix)).sorted().map(SandboxHarness::attack)
                        .toList().stream();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Attack attack(Path file) {
        try {
            String code = Files.readString(file, StandardCharsets.UTF_8);
            String first = code.lines().findFirst().orElse("");
            List<String> expected = first.startsWith("// expect:")
                    ? List.of(first.substring("// expect:".length()).trim().split("\\|")) : List.of();
            return new Attack(file.getFileName().toString(), code, expected);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 공격 하나: 파일 이름, 코드, 허용 결과(OK 또는 오류 코드) */
    public record Attack(String name, String code, List<String> expected) {

        public String resultOf(ScriptOutcome outcome) {
            return outcome.ok() ? "OK" : outcome.failure().code().name();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
