package net.java21.data2flow.flow.common;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.QuorumQueueSpec;
import net.java21.data2flow.flow.apply.repository.InstanceVersionRepository;
import net.java21.data2flow.flow.apply.service.ApplyReporter;
import net.java21.data2flow.flow.apply.service.ApplyStatusService;
import net.java21.data2flow.flow.apply.service.CleanupService;
import net.java21.data2flow.flow.apply.service.ConfigChangeListener;
import net.java21.data2flow.flow.definition.service.CachedSpaceDirectory;
import net.java21.data2flow.flow.definition.service.CoreFlowClient;
import net.java21.data2flow.flow.definition.service.CoreFlowDirectory;
import net.java21.data2flow.flow.definition.service.FlowSynchronizer;
import net.java21.data2flow.flow.messaging.AmqpDebugSink;
import net.java21.data2flow.flow.messaging.EventPublisher;
import net.java21.data2flow.flow.messaging.StreamConnection;
import net.java21.data2flow.flow.node.service.AggregateTransformNodeType;
import net.java21.data2flow.flow.node.service.ControlActionNodeType;
import net.java21.data2flow.flow.node.service.DebugLogNodeType;
import net.java21.data2flow.flow.node.service.DelayFlowNodeType;
import net.java21.data2flow.flow.node.service.JsFunctionNodeType;
import net.java21.data2flow.flow.node.service.MapTransformNodeType;
import net.java21.data2flow.flow.node.service.SpaceDirectory;
import net.java21.data2flow.flow.node.service.SwitchConditionNodeType;
import net.java21.data2flow.flow.node.service.TelemetryTriggerNodeType;
import net.java21.data2flow.flow.node.service.ThresholdConditionNodeType;
import net.java21.data2flow.flow.outbox.repository.OutboxRepository;
import net.java21.data2flow.flow.outbox.service.OutboxRelay;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.plan.service.FlowRegistry;
import net.java21.data2flow.flow.plan.service.NodeTypeRegistry;
import net.java21.data2flow.flow.runtime.domain.DebugSink;
import net.java21.data2flow.flow.runtime.repository.MetricRepository;
import net.java21.data2flow.flow.runtime.repository.NodeStateRepository;
import net.java21.data2flow.flow.runtime.repository.PartitionProgressRepository;
import net.java21.data2flow.flow.runtime.service.FlowExecutor;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import net.java21.data2flow.flow.runtime.service.JdbcExecutionStore;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import net.java21.data2flow.flow.telemetry.service.TelemetryStreamConsumer;
import net.java21.data2flow.flow.timer.repository.TimerRepository;
import org.flywaydb.core.Flyway;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.List;

/**
 * flow-engine 빈 구성: 시계, Flyway 실행 방식, 샌드박스, 노드 레지스트리·컴파일러, 실행기, core 연동, 스트림 소비자, 주기 작업(타이머·
 * 아웃박스·동기화·정리), AMQP 토폴로지.
 *
 * <p>종료 순서(graceful shutdown, reliability-and-ha.md §4): 웹 서버 → 스트림 소비자(처리 중 메시지 끝냄, phase −100) → 타이머 폴러·
 * 동기화(−200) → 아웃박스 릴레이(−300, 마지막까지 보냄) → AMQP·DB.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(FlowEngineProperties.class)
public class FlowEngineConfig {

    /** 운영 코드는 이 시계만 쓴다(ArchUnit NO_SYSTEM_CLOCK). 시험은 MutableClock으로 바꾼다 */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** ADR-030: staging과 prod가 DB 하나를 함께 쓰므로 migrate는 staging 배포와 시험에서만, 그 밖은 validate */
    @Bean
    FlywayMigrationStrategy flywayMigrationStrategy(FlowEngineProperties properties) {
        return (Flyway flyway) -> {
            if ("migrate".equalsIgnoreCase(properties.flywayMode())) {
                flyway.migrate();
            } else {
                flyway.validate();
            }
        };
    }

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    @Bean
    DeploymentScope deploymentScope() {
        return new DeploymentScope();
    }

    // ---- 노드·컴파일(FLW-02, live-reload §2) ----

    @Bean(destroyMethod = "close")
    ScriptSandbox scriptSandbox(FlowEngineProperties properties) {
        ScriptSandbox sandbox = new ScriptSandbox(properties.script().toLimits(), JsFunctionNodeType.CONTEXT_KEYS);
        // 데워질 때까지 예열한다(빈 생성이 끝나야 readiness가 열린다). 목표에 못 미치면 시간 초과 오판 위험을 경고로 남긴다
        ScriptSandbox.WarmUpResult warm = sandbox.warmUp(properties.script().toWarmUpPolicy());
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ScriptSandbox.class);
        if (warm.reachedTarget()) {
            log.info("JS 함수 노드 샌드박스 예열 완료: {}회, {}ms, 대표 스크립트 CPU {}ms", warm.rounds(), warm.elapsedMs(),
                    warm.lastCpuMs());
        } else {
            log.warn("JS 함수 노드 샌드박스 예열이 목표({})에 못 미쳤습니다: {}회, {}ms, 대표 스크립트 CPU {}ms. CPU가 부족하면 정상 "
                    + "스크립트도 시간 초과로 보일 수 있습니다", properties.script().warmUpTarget(), warm.rounds(), warm.elapsedMs(),
                    warm.lastCpuMs());
        }
        return sandbox;
    }

    @Bean
    CoreFlowDirectory coreFlowDirectory(FlowEngineProperties properties) {
        return new CoreFlowClient(properties.core());
    }

    @Bean(destroyMethod = "close")
    CachedSpaceDirectory spaceDirectory(CoreFlowDirectory core, FlowEngineProperties properties, Clock clock) {
        return new CachedSpaceDirectory(core, properties.core().spaceCacheTtl(), clock);
    }

    @Bean
    NodeTypeRegistry nodeTypeRegistry(SpaceDirectory spaces, ScriptSandbox sandbox, FlowEngineProperties properties) {
        return new NodeTypeRegistry(List.of(
                new TelemetryTriggerNodeType(spaces),
                new ThresholdConditionNodeType(),
                new SwitchConditionNodeType(),
                new MapTransformNodeType(),
                new AggregateTransformNodeType(),
                new JsFunctionNodeType(sandbox),
                new DelayFlowNodeType(),
                new ControlActionNodeType(CapabilityCatalog.standard(), properties.execution().defaultValidity()),
                new DebugLogNodeType()));
    }

    @Bean
    FlowCompiler flowCompiler(NodeTypeRegistry registry) {
        return new FlowCompiler(registry);
    }

    @Bean
    FlowRegistry flowRegistry() {
        return new FlowRegistry();
    }

    // ---- 실행(FLW-05) ----

    @Bean
    FlowExecutor flowExecutor(Clock clock, FlowEngineProperties properties) {
        return new FlowExecutor(clock, properties.execution());
    }

    @Bean
    net.java21.data2flow.flow.runtime.service.MetricBuffer metricBuffer(MetricRepository metrics) {
        return new net.java21.data2flow.flow.runtime.service.MetricBuffer(metrics);
    }

    @Bean
    JdbcExecutionStore jdbcExecutionStore(NodeStateRepository states, TimerRepository timers, OutboxRepository outbox,
                                          net.java21.data2flow.flow.runtime.service.MetricBuffer metrics, Clock clock) {
        return new JdbcExecutionStore(states, timers, outbox, metrics, clock);
    }

    /** 노드 지표를 5초마다 쓴다(종료 때 마지막으로 한 번 더) */
    @Bean
    PeriodicWorker metricWorker(net.java21.data2flow.flow.runtime.service.MetricBuffer buffer, FlowEngineProperties properties) {
        return new PeriodicWorker("flow-metrics", java.time.Duration.ofSeconds(5), stopping -> buffer.flush(),
                properties.runtimeEnabled(), SmartLifecycle.DEFAULT_PHASE - 300) {
            @Override
            public void stop() {
                super.stop();
                buffer.flush();
            }
        };
    }

    @Bean
    DebugSink debugSink(RabbitTemplate rabbit, FlowEngineProperties properties, Clock clock) {
        return new AmqpDebugSink(rabbit, properties.debug(), properties.instanceId(), clock);
    }

    @Bean
    FlowRuntimeService flowRuntimeService(FlowRegistry registry, FlowExecutor executor, JdbcExecutionStore store,
                                          PartitionProgressRepository progress, TimerRepository timers, TransactionTemplate tx,
                                          DebugSink debug, DeploymentScope scope, FlowEngineProperties properties, Clock clock,
                                          MeterRegistry meters) {
        return new FlowRuntimeService(registry, executor, store, progress, timers, tx, debug, scope, properties, clock, meters);
    }

    // ---- 적용·동기화(live-reload §4) ----

    @Bean
    EventPublisher eventPublisher(RabbitTemplate rabbit, Clock clock) {
        return new EventPublisher(rabbit, clock);
    }

    @Bean
    ApplyReporter applyReporter(InstanceVersionRepository versions, EventPublisher events, FlowEngineProperties properties,
                                Clock clock) {
        return new ApplyReporter(versions, events, properties.instanceId(), clock);
    }

    @Bean
    ApplyStatusService applyStatusService(InstanceVersionRepository versions, FlowEngineProperties properties, Clock clock) {
        return new ApplyStatusService(versions, properties.retention().staleInstance(), clock);
    }

    @Bean
    FlowSynchronizer flowSynchronizer(CoreFlowDirectory core, FlowCompiler compiler, FlowRegistry registry, ApplyReporter reporter,
                                      NodeStateRepository states, TimerRepository timers, DeploymentScope scope,
                                      FlowEngineProperties properties, Clock clock) {
        return new FlowSynchronizer(core, compiler, registry, reporter, states, timers, scope,
                properties.retention().deletedNodeState(), clock);
    }

    @Bean
    CleanupService cleanupService(TimerRepository timers, OutboxRepository outbox, NodeStateRepository states,
                                  InstanceVersionRepository versions, DeploymentScope scope, FlowEngineProperties properties,
                                  Clock clock) {
        return new CleanupService(timers, outbox, states, versions, scope, properties.retention(), properties.instanceId(), clock);
    }

    // ---- 주기 작업 ----

    @Bean
    PeriodicWorker flowSyncWorker(FlowSynchronizer synchronizer, FlowEngineProperties properties) {
        boolean[] first = {true};
        return new PeriodicWorker("flow-sync", properties.core().syncInterval(), stopping -> {
            synchronizer.syncAll(first[0]);
            first[0] = false;
        }, properties.runtimeEnabled(), SmartLifecycle.DEFAULT_PHASE - 200);
    }

    @Bean
    PeriodicWorker timerPoller(FlowRuntimeService runtime, FlowEngineProperties properties) {
        return new PeriodicWorker("flow-timers", properties.timer().pollInterval(), stopping -> {
            int fired;
            do {
                fired = runtime.fireDueTimers(properties.timer().batch(), stopping);
            } while (fired >= properties.timer().batch() && !stopping.getAsBoolean());
        }, properties.runtimeEnabled(), SmartLifecycle.DEFAULT_PHASE - 200);
    }

    @Bean
    OutboxRelay outboxRelay(OutboxRepository outbox, RabbitTemplate rabbit, TransactionTemplate tx, DeploymentScope scope,
                            FlowEngineProperties properties, Clock clock) {
        return new OutboxRelay(outbox, rabbit, tx, scope, properties.outbox(), clock);
    }

    @Bean
    PeriodicWorker outboxRelayWorker(OutboxRelay relay, FlowEngineProperties properties) {
        return new PeriodicWorker("flow-outbox", properties.outbox().pollInterval(), stopping -> {
            int sent;
            do {
                sent = relay.relayOnce(stopping);
            } while (sent >= properties.outbox().batch() && !stopping.getAsBoolean());
        }, properties.runtimeEnabled(), SmartLifecycle.DEFAULT_PHASE - 300);
    }

    @Bean
    PeriodicWorker cleanupWorker(CleanupService cleanup, FlowEngineProperties properties) {
        return new PeriodicWorker("flow-cleanup", properties.retention().cleanupInterval(), stopping -> cleanup.cleanup(),
                properties.runtimeEnabled(), SmartLifecycle.DEFAULT_PHASE - 200);
    }

    @Bean
    PeriodicWorker heartbeatWorker(CleanupService cleanup, FlowEngineProperties properties) {
        return new PeriodicWorker("flow-heartbeat", properties.retention().heartbeat(), stopping -> cleanup.heartbeat(),
                properties.runtimeEnabled(), SmartLifecycle.DEFAULT_PHASE - 200);
    }

    // ---- RabbitMQ Stream ----

    @Bean(destroyMethod = "close")
    StreamConnection streamConnection(RabbitProperties rabbit, FlowEngineProperties properties) {
        return new StreamConnection(rabbit, properties.stream());
    }

    @Bean
    TelemetryStreamConsumer telemetryStreamConsumer(StreamConnection connection, FlowRuntimeService runtime,
                                                    FlowEngineProperties properties) {
        return new TelemetryStreamConsumer(connection, runtime, properties);
    }

    /** readiness: 처음 동기화가 끝났고 소비자가 열렸는지(꺼 둔 환경은 항상 UP) */
    @Bean("flowRuntime")
    HealthIndicator flowRuntimeHealth(TelemetryStreamConsumer consumer, FlowSynchronizer synchronizer,
                                      FlowRegistry registry, FlowEngineProperties properties) {
        return () -> {
            if (!properties.runtimeEnabled()) {
                return Health.up().withDetail("runtime", "disabled").build();
            }
            if (consumer.isConsuming() && synchronizer.synced()) {
                return Health.up().withDetail("group", consumer.groupName()).withDetail("flows", registry.all().size()).build();
            }
            return Health.down().withDetail("consuming", consumer.isConsuming()).withDetail("synced", synchronizer.synced())
                    .withDetail("error", String.valueOf(consumer.lastError())).build();
        };
    }

    // ---- RabbitMQ AMQP: 행동·이벤트·설정·디버그 ----

    /** 라우팅되지 않은 행동 요청은 반환받아 실패로 보고 다시 보낸다(OutboxRelay) */
    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMandatory(true);
        return template;
    }

    @Bean
    DirectExchange actionsExchange() {
        return new DirectExchange(MessagingNames.EXCHANGE_ACTIONS, true, false);
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(MessagingNames.EXCHANGE_DLX, true, false);
    }

    /**
     * 제어 명령 큐 {@code action.commands}(소비자 action)를 생산자도 같은 인자로 선언한다: action이 아직 뜨지 않았어도 행동 요청이 큐에
     * 남아 유실되지 않는다(QuorumQueueSpec 인자가 같아 선언이 겹쳐도 충돌하지 않음).
     */
    @Bean
    Queue actionCommandsQueue() {
        QuorumQueueSpec spec = QuorumQueueSpec.ACTION_COMMANDS;
        return QueueBuilder.durable(spec.name()).withArguments(spec.arguments()).build();
    }

    @Bean
    Binding actionCommandsBinding(Queue actionCommandsQueue, DirectExchange actionsExchange) {
        return BindingBuilder.bind(actionCommandsQueue).to(actionsExchange).with(QuorumQueueSpec.ACTION_COMMANDS.routingKey());
    }

    @Bean
    Queue actionCommandsDeadLetterQueue() {
        return QueueBuilder.durable(QuorumQueueSpec.ACTION_COMMANDS.deadLetterQueue())
                .withArguments(QuorumQueueSpec.deadLetterArguments()).build();
    }

    @Bean
    Binding actionCommandsDeadLetterBinding(Queue actionCommandsDeadLetterQueue, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(actionCommandsDeadLetterQueue).to(deadLetterExchange).with(QuorumQueueSpec.ACTION_COMMANDS.name());
    }

    @Bean
    TopicExchange eventsExchange() {
        return new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false);
    }

    @Bean
    TopicExchange debugExchange() {
        return new TopicExchange(MessagingNames.EXCHANGE_DEBUG, true, false);
    }

    @Bean
    FanoutExchange configExchange() {
        return new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false);
    }

    @Bean
    AnonymousQueue flowConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("flow.config."));
    }

    @Bean
    Binding flowConfigBinding(AnonymousQueue flowConfigQueue, FanoutExchange configExchange) {
        return BindingBuilder.bind(flowConfigQueue).to(configExchange);
    }

    @Bean
    ConfigChangeListener configChangeListener(FlowSynchronizer synchronizer, SpaceDirectory spaces) {
        return new ConfigChangeListener(synchronizer, spaces);
    }

    @Bean
    net.java21.data2flow.flow.apply.service.ConfigReconnectHandler configReconnectHandler(ConfigChangeListener listener) {
        return new net.java21.data2flow.flow.apply.service.ConfigReconnectHandler(listener);
    }

    @Bean
    SimpleMessageListenerContainer configChangeContainer(ConnectionFactory connectionFactory, AnonymousQueue flowConfigQueue,
                                                         ConfigChangeListener listener, FlowEngineProperties properties) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(flowConfigQueue);
        container.setMessageListener(listener);
        container.setMissingQueuesFatal(false);
        container.setAutoStartup(properties.runtimeEnabled());
        return container;
    }
}
