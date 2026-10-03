package net.java21.data2flow.flow.common;

import net.java21.data2flow.flow.script.domain.ScriptLimits;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * flow-engine 설정({@code data2flow.flow.*}, application.yml).
 *
 * @param flywayMode     migrate(staging·테스트) 또는 validate(prod·local), ADR-030
 * @param instanceId     인스턴스 이름(파드 이름). {@code flow_instance_versions.instance_id}, 적용 보고
 * @param developer      로컬 개발자 이름(소비자 그룹 접미사). 운영·staging은 빈 값
 * @param runtimeEnabled 스트림 소비·지속 타이머·아웃박스 발행·설정 수신을 켤지. 로컬은 끈다(공용 DB의 운영 타이머 보호)
 */
@ConfigurationProperties("data2flow.flow")
public record FlowEngineProperties(String flywayMode, String instanceId, String developer, boolean runtimeEnabled,
                                   Core core, Stream stream, Timer timer, Outbox outbox, Execution execution,
                                   Debug debug, Retention retention, Script script) {

    /** core-api 내부 API(API-FLW-80·81, API-DEV-128) */
    public record Core(String baseUrl, Duration connectTimeout, Duration readTimeout, Duration syncInterval,
                       Duration spaceCacheTtl) {
    }

    /** RabbitMQ Stream. 테스트는 partitions 3, fixed-address true(단일 노드 s4) */
    public record Stream(int port, int partitions, boolean fixedAddress) {
    }

    /** 지속 타이머 폴러(flow_timers, SKIP LOCKED) */
    public record Timer(Duration pollInterval, int batch, int maxAttempts) {
    }

    /** 아웃박스 릴레이(flow_outboxes → data2flow.actions, publisher confirm 후 sent_at) */
    public record Outbox(Duration pollInterval, int batch, Duration confirmTimeout) {
    }

    /**
     * 메시지 실행.
     *
     * @param retryInitial   일시 장애 재시도 첫 대기
     * @param retryMax       재시도 최대 대기
     * @param maxHops        메시지 하나가 거치는 노드 수 상한(BR-FLW-16)
     * @param maxFanout      노드 하나가 입력 하나에 내보내는 메시지 수 상한(BR-FLW-16)
     * @param maxStateBytes  노드 상태 대상 키당 상한(BR-FLW-29, 256KB)
     * @param defaultValidity 제어 명령 유효 시간 기본값(validitySeconds 없을 때)
     */
    public record Execution(Duration retryInitial, Duration retryMax, int maxHops, int maxFanout, int maxStateBytes,
                            Duration defaultValidity) {
    }

    /** 디버그 샘플 상한(BR-FLW-12): 노드당 초당 5건, 디버그 켠 노드 50건 */
    public record Debug(int samplesPerSecond, int debugSamplesPerSecond) {
    }

    /** 보관·정리 */
    public record Retention(Duration deletedNodeState, Duration finishedTimers, Duration sentOutboxes,
                            Duration staleInstance, Duration heartbeat, Duration cleanupInterval) {
    }

    /** JS 함수 노드 실행 제한(SCR-02.02와 같은 값) */
    public record Script(Duration cpuTime, Duration wallTime, long statementLimit, int maxOutputBytes, int maxLogBytes,
                         int maxLogEntries, int maxStringLength, int maxArrayLength, int maxCodeBytes,
                         int maxOutputDepth, int warmUpRounds) {

        public ScriptLimits toLimits() {
            return new ScriptLimits(cpuTime, wallTime, statementLimit, maxOutputBytes, maxLogBytes, maxLogEntries,
                    maxStringLength, maxArrayLength, maxCodeBytes, maxOutputDepth);
        }
    }
}
