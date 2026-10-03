package net.java21.data2flow.flow.runtime.domain;

import net.java21.data2flow.flow.plan.domain.DebugSample;

import java.util.List;
import java.util.UUID;

/** 커밋 뒤 라이브 뷰 샘플을 내보내는 곳(EVT-FLW-01, 손실 허용) */
public interface DebugSink {

    void publish(UUID flowId, int version, List<DebugSample> samples);

    DebugSink NONE = (flowId, version, samples) -> {
    };
}
