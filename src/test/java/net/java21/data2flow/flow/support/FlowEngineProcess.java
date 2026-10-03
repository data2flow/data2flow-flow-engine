package net.java21.data2flow.flow.support;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * flow-engine을 <b>별도 JVM</b>으로 띄운다(M3 완료 확인 "flow-engine 두 대 중 하나를 kill -9"): 시험 클래스패스와 test 프로필을 그대로 쓰고
 * 인프라 주소는 Testcontainers 값으로 넘긴다. {@link #kill()}은 {@code destroyForcibly()}(SIGKILL, 정리 없음)다. DB 연결의
 * ApplicationName이 인스턴스 이름이라 시험이 {@code pg_stat_activity}로 잠금을 쥔 인스턴스를 찾을 수 있다.
 */
public final class FlowEngineProcess implements AutoCloseable {

    private final String name;
    private final Process process;
    private final int managementPort;
    private final Path log;

    private FlowEngineProcess(String name, Process process, int managementPort, Path log) {
        this.name = name;
        this.process = process;
        this.managementPort = managementPort;
        this.log = log;
    }

    public static FlowEngineProcess start(String name, String vhost, String coreBaseUrl) {
        try {
            int management = freePort();
            Path log = Files.createTempFile("flow-engine-" + name, ".log");
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            Path argFile = Files.createTempFile("flow-engine-" + name, ".args");
            Files.writeString(argFile, "-cp \"" + classpath.replace("\\", "\\\\") + "\"");
            String jdbc = TestInfrastructure.POSTGRES.getJdbcUrl();
            jdbc += (jdbc.contains("?") ? "&" : "?") + "ApplicationName=" + name;
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx384m", "-Dpolyglotimpl.AttachLibraryFailureAction=ignore", "@" + argFile,
                    "net.java21.data2flow.flow.FlowEngineApplication",
                    "--spring.profiles.active=test",
                    "--spring.datasource.url=" + jdbc,
                    "--spring.datasource.username=" + TestInfrastructure.POSTGRES.getUsername(),
                    "--spring.datasource.password=" + TestInfrastructure.POSTGRES.getPassword(),
                    "--spring.rabbitmq.host=" + TestInfrastructure.rabbitHost(),
                    "--spring.rabbitmq.port=" + TestInfrastructure.amqpPort(),
                    "--spring.rabbitmq.username=guest", "--spring.rabbitmq.password=guest",
                    "--spring.rabbitmq.virtual-host=" + vhost,
                    "--data2flow.flow.stream.port=" + TestInfrastructure.streamPort(),
                    "--data2flow.flow.core.base-url=" + coreBaseUrl,
                    "--data2flow.flow.instance-id=" + name,
                    "--data2flow.flow.script.warm-up-rounds=1",
                    "--server.port=0",
                    "--management.server.port=" + management));
            Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
            return new FlowEngineProcess(name, process, management, log);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** readiness(처음 동기화 + 텔레메트리 소비자)가 UP이 될 때까지 */
    public FlowEngineProcess awaitReady(Duration timeout) {
        HttpClient http = HttpClient.newHttpClient();
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().until(() -> {
            if (!process.isAlive()) {
                throw new IllegalStateException(name + " 프로세스가 끝났습니다:\n" + tail());
            }
            HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create(
                    "http://localhost:" + managementPort + "/actuator/health/readiness")).GET().build(), HttpResponse.BodyHandlers.ofString());
            return res.statusCode() == 200;
        });
        return this;
    }

    public String name() {
        return name;
    }

    /** kill -9(정리 없이 즉시 종료) */
    public void kill() {
        process.toHandle().destroyForcibly();
        await().atMost(Duration.ofSeconds(30)).until(() -> !process.isAlive());
    }

    public boolean alive() {
        return process.isAlive();
    }

    public String logText() {
        try {
            return Files.readString(log);
        } catch (IOException e) {
            return "";
        }
    }

    public String tail() {
        try {
            List<String> lines = Files.readAllLines(log);
            return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
        } catch (IOException e) {
            return "";
        }
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(30, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
