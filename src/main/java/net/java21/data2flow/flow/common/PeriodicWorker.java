package net.java21.data2flow.flow.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 주기 작업(타이머 폴러·아웃박스 릴레이·설정 동기화·정리). 전용 스레드 하나가 앞 실행이 끝난 뒤 간격만큼 쉬고 다시 실행한다.
 * 종료(graceful shutdown) 때는 새 실행을 멈추고 진행 중인 실행이 끝나기를 기다린다(트랜잭션 중간에 끊지 않음).
 */
public class PeriodicWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(PeriodicWorker.class);

    /** 작업: 종료 신호를 받아 긴 반복을 멈출 수 있다 */
    @FunctionalInterface
    public interface Task {
        void run(BooleanSupplier stopping);
    }

    private final String name;
    private final Duration interval;
    private final Task task;
    private final boolean autoStartup;
    private final int phase;
    private volatile ScheduledExecutorService executor;
    private volatile boolean stopping;

    public PeriodicWorker(String name, Duration interval, Task task, boolean autoStartup, int phase) {
        this.name = name;
        this.interval = interval;
        this.task = task;
        this.autoStartup = autoStartup;
        this.phase = phase;
    }

    @Override
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        stopping = false;
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name(name).factory());
        executor.scheduleWithFixedDelay(() -> {
            try {
                task.run(() -> stopping);
            } catch (RuntimeException e) {
                log.warn("{} 실행 실패(다음 주기에 다시): {}", name, e.toString());
            }
        }, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void stop() {
        stopping = true;
        ScheduledExecutorService e = executor;
        executor = null;
        if (e != null) {
            e.shutdown();
            try {
                if (!e.awaitTermination(20, TimeUnit.SECONDS)) {
                    e.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                e.shutdownNow();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    public String name() {
        return name;
    }
}
