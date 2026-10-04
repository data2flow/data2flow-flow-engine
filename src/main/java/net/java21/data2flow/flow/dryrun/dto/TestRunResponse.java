package net.java21.data2flow.flow.dryrun.dto;

import net.java21.data2flow.contracts.flow.FlowTrace;

/** API-FLW-12 응답 {@code {trace}}(행동 노드 결과는 {@code action.dryRun:true}) */
public record TestRunResponse(FlowTrace trace) {
}
