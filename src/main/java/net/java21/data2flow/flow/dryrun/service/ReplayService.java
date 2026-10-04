package net.java21.data2flow.flow.dryrun.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.flow.FlowDefinition;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.flow.common.DeploymentScope;
import net.java21.data2flow.flow.common.FlowEngineErrorCode;
import net.java21.data2flow.flow.definition.service.CoreFlowDirectory;
import net.java21.data2flow.flow.dryrun.dto.ReplayRequest;
import net.java21.data2flow.flow.dryrun.repository.ReplayJobRepository;
import net.java21.data2flow.flow.plan.domain.ExecutionPlan;
import net.java21.data2flow.flow.plan.domain.FlowMessage;
import net.java21.data2flow.flow.plan.domain.Jsons;
import net.java21.data2flow.flow.plan.domain.Overlay;
import net.java21.data2flow.flow.plan.domain.PlanNode;
import net.java21.data2flow.flow.plan.domain.TriggerNode;
import net.java21.data2flow.flow.plan.service.FlowCompiler;
import net.java21.data2flow.flow.runtime.domain.ActionGuard;
import net.java21.data2flow.flow.runtime.domain.ExecutionReport;
import net.java21.data2flow.flow.runtime.domain.InMemoryExecutionStore;
import net.java21.data2flow.flow.runtime.service.FlowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * 과거 데이터 재생(FLW-03.06, API-FLW-13, BR-FLW-11): 지정한 기간(7일 이하)의 실제 텔레메트리를 측정 시각 순서로 플로우에 넣어 드라이런하고
 * "제어 12회, 알림 3회"처럼 행동 노드별 예상 실행 횟수를 요약한다. 실제 명령·알림·Sink는 0건이고 운영 노드 상태는 바뀌지 않는다(메모리 저장소).
 * 지속 타이머는 재생 중인 시각(측정 시각)으로 발화한다.
 *
 * <ul>
 *   <li>요청은 작업으로 저장하고(202 {jobId}) 인스턴스의 재생 작업자가 하나씩 실행한다. 진행·결과는 {@code flow_replay_jobs}.</li>
 *   <li>데이터는 core-api 내부 API로 쪽 단위(1,000건)로 읽는다(엔진은 pipeline 스키마를 읽지 않음). 재처리된 텔레메트리는
 *       {@code data2flow.telemetry}에 다시 나오지 않으므로 운영 플로우는 반응하지 않는다(ADR-048 남은 것 ①의 결정).</li>
 *   <li>결과: {@code {executions, branchCounts:{nodeId:{port:count}}, actions:{command, notify, sink}, errors}}.</li>
 * </ul>
 */
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);
    public static final Duration MAX_RANGE = Duration.ofDays(7);
    static final long MAX_MESSAGES = 1_000_000;
    static final int PAGE_SIZE = 1000;
    static final Duration STALE = Duration.ofMinutes(1);
    static final int MAX_TIMER_FIRES_PER_STEP = 10_000;

    private final ReplayJobRepository jobs;
    private final FlowCompiler compiler;
    private final FlowExecutor executor;
    private final CoreFlowDirectory core;
    private final DeploymentScope scope;
    private final String instanceId;
    private final Clock clock;

    public ReplayService(ReplayJobRepository jobs, FlowCompiler compiler, FlowExecutor executor, CoreFlowDirectory core,
                         DeploymentScope scope, String instanceId, Clock clock) {
        this.jobs = jobs;
        this.compiler = compiler;
        this.executor = executor;
        this.core = core;
        this.scope = scope;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    /** 작업을 만든다(검증 실패는 400). 작업 ID */
    public long submit(ReplayRequest request) {
        UUID flowId = TestRunService.parseFlowId(request.flowId());
        long organizationId;
        try {
            organizationId = Long.parseLong(request.organizationId());
        } catch (NumberFormatException | NullPointerException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("organizationId", "INVALID",
                    "organizationId가 필요합니다")));
        }
        if (request.from() == null || request.to() == null || !request.from().isBefore(request.to())) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("to", "INVALID",
                    "from < to여야 합니다")));
        }
        if (Duration.between(request.from(), request.to()).compareTo(MAX_RANGE) > 0) {
            throw new BusinessException(FlowEngineErrorCode.FLOW_REPLAY_TOO_LARGE, List.of(new FieldErrorDetail("to", "LIMIT",
                    "재생 기간은 7일 이하입니다")));
        }
        compile(flowId, organizationId, request);   // 정의가 틀리면 지금 400
        scope.add(organizationId);
        return jobs.insert(organizationId, flowId, Jsons.MAPPER.valueToTree(request), clock.instant());
    }

    private ExecutionPlan compile(UUID flowId, long organizationId, ReplayRequest request) {
        FlowDefinition definition = request.definition();
        if (definition == null) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("definition", "REQUIRED",
                    "재생할 버전의 정의가 필요합니다")));
        }
        FlowCompiler.Result r = compiler.compile(flowId, organizationId, request.version() == null ? 0 : request.version(),
                definition);
        if (!r.ok()) {
            throw new BusinessException(FlowEngineErrorCode.FLOW_VALIDATION_FAILED, r.errors().stream()
                    .map(e -> new FieldErrorDetail(e.field(), e.code(), e.message())).toList());
        }
        return r.plan();
    }

    public Optional<ReplayJobRepository.Job> find(long jobId) {
        return jobs.find(jobId);
    }

    public boolean cancel(long jobId) {
        return jobs.find(jobId).map(j -> jobs.cancel(j.organizationId(), jobId, clock.instant())).orElse(false);
    }

    /** 작업 하나를 잡아 끝까지 실행한다(재생 작업자). 실행했으면 true */
    public boolean runNext(BooleanSupplier stopping) {
        Optional<ReplayJobRepository.Job> claimed = jobs.claim(scope.organizations(), instanceId, clock.instant(),
                clock.instant().minus(STALE));
        if (claimed.isEmpty()) {
            return false;
        }
        ReplayJobRepository.Job job = claimed.get();
        Progress p = new Progress();
        try {
            ReplayRequest request = Jsons.MAPPER.treeToValue(job.request(), ReplayRequest.class);
            ObjectNode result = run(job, request, p, stopping);
            if (result == null) {
                log.info("과거 재생 {} 중단(취소·종료)", job.id());
                return true;
            }
            jobs.finish(job.organizationId(), job.id(), "SUCCEEDED", result, null, p.processed, clock.instant());
            log.info("과거 재생 {} 완료: 메시지 {}, 실행 {}", job.id(), p.processed, result.path("executions").asLong());
        } catch (BusinessException e) {
            jobs.finish(job.organizationId(), job.id(), "FAILED", null, e.getErrorCode().code(), p.processed, clock.instant());
        } catch (RuntimeException e) {
            log.warn("과거 재생 {} 실패: {}", job.id(), e.toString());
            jobs.finish(job.organizationId(), job.id(), "FAILED", null, e.getMessage(), p.processed, clock.instant());
        }
        return true;
    }

    private static final class Progress {
        long processed;
    }

    /** 결과. 취소되었거나 종료 중이면 null */
    ObjectNode run(ReplayJobRepository.Job job, ReplayRequest request, Progress progress, BooleanSupplier stopping) {
        ExecutionPlan plan = compile(job.flowId(), job.organizationId(), request);
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        FlowExecutor.RunOptions options = new FlowExecutor.RunOptions(true, request.from(), ActionGuard.NONE);
        Summary summary = new Summary();
        List<Long> devices = request.deviceIds() == null ? List.of()
                : request.deviceIds().stream().map(Long::parseLong).toList();
        String cursor = null;
        Long total = null;
        do {
            if (stopping.getAsBoolean()) {
                return null;
            }
            CoreFlowDirectory.TelemetryPage page = core.telemetryHistory(job.organizationId(), devices, request.from(), request.to(),
                    cursor, PAGE_SIZE);
            total = page.total() != null ? page.total() : total;
            if (total != null && total > MAX_MESSAGES) {
                throw new BusinessException(FlowEngineErrorCode.FLOW_REPLAY_TOO_LARGE);
            }
            for (CanonicalTelemetry t : page.items()) {
                Instant at = t.measuredAt();
                summary.addAll(TestRunService.fireUntil(executor, plan, store, at, options, MAX_TIMER_FIRES_PER_STEP));
                for (PlanNode trigger : plan.triggers()) {
                    Optional<FlowMessage> m = ((TriggerNode) trigger.compiled()).match(t);
                    if (m.isPresent()) {
                        FlowMessage message = m.get().withRunKey(plan.mode().runKey(m.get()));
                        summary.add(executor.runTrigger(plan, Overlay.NONE, trigger, message, store, options.at(at)));
                    }
                }
                progress.processed++;
                if (progress.processed > MAX_MESSAGES) {
                    throw new BusinessException(FlowEngineErrorCode.FLOW_REPLAY_TOO_LARGE);
                }
            }
            if (!jobs.progress(job.organizationId(), job.id(), progress.processed, total, clock.instant())) {
                return null;   // 취소됨
            }
            cursor = page.nextCursor();
        } while (cursor != null);
        summary.addAll(TestRunService.fireUntil(executor, plan, store, request.to(), options, MAX_TIMER_FIRES_PER_STEP));
        return summary.toJson();
    }

    /** 재생 요약 */
    static final class Summary {
        long executions;
        long errors;
        long command;
        long notify;
        long sink;
        long alarms;
        final Map<String, Map<String, Long>> branches = new LinkedHashMap<>();

        void addAll(List<ExecutionReport> reports) {
            reports.forEach(this::add);
        }

        void add(ExecutionReport r) {
            executions++;
            if (r.failed()) {
                errors++;
            }
            for (ExecutionReport.Step s : r.steps()) {
                for (String port : s.ports()) {
                    if (!"retry".equals(port)) {
                        branches.computeIfAbsent(s.nodeId(), k -> new LinkedHashMap<>()).merge(port, 1L, Long::sum);
                    }
                }
                for (ExecutionReport.ActionRecord a : s.actions()) {
                    if (a.skipped() != null) {
                        continue;
                    }
                    switch (a.kind()) {
                        case "COMMAND", "SCENE" -> command++;
                        case "NOTIFY" -> notify++;
                        case "SINK" -> sink++;
                        default -> alarms++;
                    }
                }
            }
        }

        ObjectNode toJson() {
            ObjectNode out = Jsons.object();
            out.put("executions", executions);
            ObjectNode b = out.putObject("branchCounts");
            branches.forEach((node, ports) -> {
                ObjectNode p = b.putObject(node);
                ports.forEach(p::put);
            });
            out.putObject("actions").put("command", command).put("notify", notify).put("sink", sink).put("alarm", alarms);
            out.put("errors", errors);
            return out;
        }
    }

    /** 응답 모양 {@code {jobId, flowId, status, progress:{processed, total}, result, error}} */
    public static JsonNode view(ReplayJobRepository.Job job) {
        ObjectNode out = Jsons.object();
        out.put("jobId", Long.toString(job.id()));
        out.put("flowId", job.flowId().toString());
        out.put("status", job.status());
        ObjectNode p = out.putObject("progress").put("processed", job.processed());
        if (job.total() != null) {
            p.put("total", job.total());
        } else {
            p.putNull("total");
        }
        out.set("result", job.result());
        out.put("error", job.error());
        return out;
    }

    /** 끝난 작업 정리(7일) */
    public int cleanup() {
        return jobs.deleteFinished(clock.instant().minus(Duration.ofDays(7)));
    }

}
