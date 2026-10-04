package net.java21.data2flow.flow.liveview.service;

import net.java21.data2flow.flow.liveview.repository.TraceRepository;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

/** 실행 추적 조회(API-FLW-41). 1시간이 지난 추적은 보이지 않는다(FLW-03.04) */
public class TraceQueryService {

    private final TraceRepository repository;
    private final Clock clock;

    public TraceQueryService(TraceRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    public Optional<JsonNode> find(String messageId, UUID flowId) {
        return repository.find(messageId, flowId, clock.instant());
    }
}
