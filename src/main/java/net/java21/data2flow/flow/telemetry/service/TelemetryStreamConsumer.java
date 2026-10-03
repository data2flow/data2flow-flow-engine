package net.java21.data2flow.flow.telemetry.service;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.messaging.ConsumerGroups;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.flow.common.Backoff;
import net.java21.data2flow.flow.common.FlowEngineProperties;
import net.java21.data2flow.flow.messaging.StreamConnection;
import net.java21.data2flow.flow.runtime.service.FlowRuntimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code data2flow.telemetry} Super Stream 소비자(FLW-05.01, design/flow-engine-and-live-reload.md §3, reliability-and-ha.md §2).
 *
 * <ul>
 *   <li>그룹 {@code flow}(로컬 {@code flow-<개발자>}), Single Active Consumer: 파티션마다 활성 소비자 하나가 순서대로 처리하므로 같은 기기의
 *       메시지는 순서대로, 기기 단위 queued(BR-FLW-18 기본). 인스턴스가 죽으면 브로커가 대기 소비자를 활성화한다.</li>
 *   <li>파티션별 순차 디스패처(live-reload §3): 스트림 클라이언트의 전달 스레드는 메시지를 파티션별 작업 스레드(가상 스레드 하나)에 넘기기만
 *       하므로 파티션끼리는 동시에, 파티션 안에서는 순서대로 처리된다. 파티션당 대기 {@value #MAX_QUEUED}건을 넘으면 전달을 멈춘다(배압).
 *       파티션을 다른 인스턴스가 넘겨받으면 아직 처리하지 않은 대기 메시지는 버린다(오프셋을 저장하지 않으므로 넘겨받은 쪽이 처리).</li>
 *   <li>오프셋은 관심 있는 모든 플로우의 처리 트랜잭션이 커밋된 뒤에만 저장한다(수동 추적). 넘겨받은 소비자는 저장된 오프셋 다음부터 읽고,
 *       저장된 오프셋이 없으면 지금부터 읽는다(지난 데이터로 행동이 나가지 않게).</li>
 *   <li>읽을 수 없는 메시지(형식 오류·모르는 버전)는 건너뛴다(pipeline이 이미 검증한 메시지이고, 원본은 pipeline에 남아 있다).</li>
 *   <li>종료: 새 메시지를 받지 않고 처리 중인 것을 끝낸 뒤 소비자를 닫는다(브로커가 다른 인스턴스를 활성화).</li>
 * </ul>
 */
public class TelemetryStreamConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TelemetryStreamConsumer.class);
    static final int MAX_QUEUED = 256;

    private final StreamConnection connection;
    private final FlowRuntimeService runtime;
    private final FlowEngineProperties properties;
    private final MessageCodec codec = MessageCodec.create();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final java.util.Map<Integer, PartitionWorker> workers = new java.util.concurrent.ConcurrentHashMap<>();
    private final ScheduledExecutorService starter =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("telemetry-consumer-start").factory());
    private volatile Consumer consumer;
    private volatile boolean running;
    private volatile boolean stopping;
    private volatile String lastError;

    public TelemetryStreamConsumer(StreamConnection connection, FlowRuntimeService runtime, FlowEngineProperties properties) {
        this.connection = connection;
        this.runtime = runtime;
        this.properties = properties;
    }

    public String groupName() {
        return ConsumerGroups.of(ConsumerGroups.FLOW, properties.developer());
    }

    @Override
    public boolean isAutoStartup() {
        return properties.runtimeEnabled();
    }

    @Override
    public void start() {
        running = true;
        stopping = false;
        starter.execute(this::connect);
    }

    private void connect() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(30));
        while (running && consumer == null) {
            try {
                consumer = connection.environment().consumerBuilder()
                        .superStream(MessagingNames.STREAM_TELEMETRY)
                        .name(groupName())
                        .singleActiveConsumer()
                        .manualTrackingStrategy().builder()
                        .consumerUpdateListener(context -> {
                            worker(StreamConnection.partitionIndex(context.stream())).newGeneration();
                            if (!context.isActive()) {
                                log.info("파티션 {} 비활성(다른 인스턴스가 넘겨받음)", context.stream());
                                return null;
                            }
                            try {
                                long stored = context.consumer().storedOffset();
                                log.info("파티션 {} 활성: 저장된 오프셋 {} 다음부터", context.stream(), stored);
                                return OffsetSpecification.offset(stored + 1);
                            } catch (NoOffsetException e) {
                                log.info("파티션 {} 활성: 저장된 오프셋 없음, 지금부터", context.stream());
                                return OffsetSpecification.next();
                            }
                        })
                        .messageHandler(this::handle)
                        .build();
                lastError = null;
                log.info("data2flow.telemetry 소비 시작(그룹 {})", groupName());
            } catch (RuntimeException e) {
                lastError = e.getMessage();
                log.warn("data2flow.telemetry 소비자를 열지 못했습니다(다시 시도): {}", e.getMessage());
                if (!backoff.pause(() -> !running)) {
                    return;
                }
            }
        }
    }

    private PartitionWorker worker(int partition) {
        return workers.computeIfAbsent(partition, PartitionWorker::new);
    }

    private void handle(MessageHandler.Context context, Message message) {
        if (stopping) {
            return; // 오프셋을 저장하지 않으므로 넘겨받은 인스턴스가 다시 처리한다
        }
        int partition = StreamConnection.partitionIndex(context.stream());
        worker(partition).submit(context, message);
    }

    private void process(MessageHandler.Context context, Message message, int partition) {
        if (stopping) {
            return;
        }
        inFlight.incrementAndGet();
        try {
            CanonicalTelemetry telemetry;
            try {
                telemetry = codec.read(message.getBodyAsBinary(), CanonicalTelemetry.class);
            } catch (MessageFormatException e) {
                log.warn("읽을 수 없는 텔레메트리를 건너뜁니다({} 오프셋 {}): {}", context.stream(), context.offset(), e.getMessage());
                context.storeOffset();
                return;
            }
            if (runtime.processTelemetry(telemetry, partition, context.offset(), () -> stopping || !running)) {
                context.storeOffset();
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    @Override
    public void stop() {
        stopping = true;
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        Backoff wait = new Backoff(Duration.ofMillis(20), Duration.ofMillis(200));
        while (inFlight.get() > 0 && System.nanoTime() < deadline) {
            wait.pause(() -> inFlight.get() == 0);
        }
        Consumer c = consumer;
        consumer = null;
        workers.values().forEach(PartitionWorker::close);
        if (c != null) {
            try {
                c.close();
            } catch (RuntimeException e) {
                log.debug("소비자 닫기 실패: {}", e.getMessage());
            }
        }
        running = false;
        starter.shutdownNow();
        try {
            starter.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** 웹 서버보다 나중에, DB·AMQP보다 먼저 멈춘다 */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 100;
    }

    /** 파티션 하나의 순차 작업 스레드 */
    private final class PartitionWorker {
        private final int partition;
        private final java.util.concurrent.ExecutorService executor;
        private final java.util.concurrent.Semaphore slots = new java.util.concurrent.Semaphore(MAX_QUEUED);
        private final AtomicInteger generation = new AtomicInteger();

        PartitionWorker(int partition) {
            this.partition = partition;
            this.executor = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("flow-partition-" + partition).factory());
        }

        void newGeneration() {
            generation.incrementAndGet();
        }

        void submit(MessageHandler.Context context, Message message) {
            int gen = generation.get();
            try {
                slots.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                executor.execute(() -> {
                    try {
                        if (gen == generation.get()) {
                            process(context, message, partition);
                        }
                    } finally {
                        slots.release();
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException e) {
                slots.release();
            }
        }

        void close() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    public boolean isConsuming() {
        return consumer != null;
    }

    public String lastError() {
        return lastError;
    }
}
