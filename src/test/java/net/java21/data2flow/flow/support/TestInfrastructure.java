package net.java21.data2flow.flow.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.time.Duration;

/**
 * 통합 시험 인프라(FLW test-plan "Testcontainers PostgreSQL 18 + RabbitMQ 3.13(stream)"): JVM에 하나씩만 띄워 모든 IT가 함께 쓴다.
 * 실제 s3·s4 인프라에는 붙지 않는다. PostgreSQL은 {@code client_connection_check_interval}을 켜서, 잠금을 기다리던 엔진 프로세스가
 * kill -9로 사라지면 그 트랜잭션을 바로 되돌린다(TC-FLW-098).
 */
public final class TestInfrastructure {

    public static final int STREAM_PORT = 5552;

    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("data2flow")
            .withCommand("postgres", "-c", "fsync=off", "-c", "client_connection_check_interval=500", "-c", "max_connections=200");
    @SuppressWarnings("resource")
    public static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:3.13-management")
            .withExposedPorts(STREAM_PORT, 5672, 15672)
            .withCopyToContainer(Transferable.of("[rabbitmq_management,rabbitmq_stream]."), "/etc/rabbitmq/enabled_plugins")
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
    public static final CoreApiStub CORE = new CoreApiStub();

    static {
        POSTGRES.start();
        RABBIT.start();
    }

    private TestInfrastructure() {
    }

    public static String rabbitHost() {
        return RABBIT.getHost();
    }

    public static int amqpPort() {
        return RABBIT.getMappedPort(5672);
    }

    public static int streamPort() {
        return RABBIT.getMappedPort(STREAM_PORT);
    }

    /** 시험끼리 섞이지 않도록 별도 vhost를 만든다 */
    public static void createVhost(String vhost) {
        try {
            exec("rabbitmqctl", "add_vhost", vhost);
            exec("rabbitmqctl", "set_permissions", "-p", vhost, "guest", ".*", ".*", ".*");
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("vhost를 만들지 못했습니다: " + vhost, e);
        }
    }

    private static void exec(String... command) throws IOException, InterruptedException {
        var result = RABBIT.execInContainer(command);
        if (result.getExitCode() != 0 && !result.getStderr().contains("already exists")) {
            throw new IllegalStateException(String.join(" ", command) + ": " + result.getStderr());
        }
    }
}
