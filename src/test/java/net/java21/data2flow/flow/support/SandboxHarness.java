package net.java21.data2flow.flow.support;

import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.node.service.JsFunctionNodeType;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * 샌드박스 하네스: 운영과 같은 {@link ScriptSandbox}(공용 모듈 data2flow-script-sandbox, ADR-046)를 운영 설정 파일
 * (application.yml {@code data2flow.flow.script})의 한도·예열 정책과 JS 함수 노드의 ctx 추가 필드(flow·node)로 만든다.
 * 공격 코퍼스 42종과 샌드박스 자체 시험(TC-FLW-046의 샌드박스 부분)은 공용 모듈로 옮겼다.
 */
public final class SandboxHarness {

    private static volatile ScriptSandbox sandbox;

    private SandboxHarness() {
    }

    public static synchronized ScriptSandbox sandbox() {
        if (sandbox == null) {
            FlowEngineProperties.Script script = properties().script();
            sandbox = new ScriptSandbox(script.toLimits(), JsFunctionNodeType.CONTEXT_KEYS);
            sandbox.warmUp(script.toWarmUpPolicy());
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
}
