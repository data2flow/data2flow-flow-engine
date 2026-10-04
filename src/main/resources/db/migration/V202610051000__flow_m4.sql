-- data2flow_flow M4(자동화 완성) 확장: 라이브 편집 상태 지문, 재시도·결과 대기 타이머, 아웃박스 재시도 간격, 지연 분포, 실행 모드 대기열,
-- 실행 추적, 과거 재생 작업. ADR-030: staging·prod가 DB를 함께 쓰므로 추가(expand)만 한다. 이전 코드(M3)는 새 열을 읽지 않고, 새 열은
-- 모두 NULL 허용이거나 기본값이 있어 M3 코드의 INSERT가 그대로 동작한다.

-- FLW-06.03 상태 이어받기: 상태를 쓴 노드의 상태 지문({"@type", KEEP이 아닌 설정 값}). 읽을 때 지금 노드와 다르면 KEEP/RESET/MIGRATE 적용.
-- NULL(M3 행)은 지금 설정으로 쓴 것으로 본다
ALTER TABLE flow_node_state ADD COLUMN state_config jsonb;
COMMENT ON COLUMN flow_node_state.state_config IS '상태를 쓴 노드의 상태 지문(노드 종류 + 정책이 KEEP이 아닌 설정 값, FLW-06.03·BR-FLW-07)';

-- FLW-08.01 재시도(RETRY), action.control ok·failed 결과 대기(AWAIT_RESULT). CHECK를 넓히기만 한다(M3 값은 그대로 허용)
ALTER TABLE flow_timers DROP CONSTRAINT ck_flow_timers_kind;
ALTER TABLE flow_timers ADD CONSTRAINT ck_flow_timers_kind
    CHECK (kind IN ('DELAY','WAIT_UNTIL','RECHECK','APPROVAL','RETRY','AWAIT_RESULT'));
-- EVT-ACT-01 결과로 대기 타이머를 찾는다(멱등 키 = 행동 요청 키)
CREATE INDEX ix_flow_timers_await_key ON flow_timers ((context->>'awaitKey')) WHERE status = 'WAITING' AND kind = 'AWAIT_RESULT';
-- FLW-05.07 실행 모드: 실행 키별 진행 중인 실행(대기·재시도·결과 대기 타이머)
CREATE INDEX ix_flow_timers_run_key ON flow_timers (flow_id, (context->'@run'->>'key')) WHERE status = 'WAITING';

-- 아웃박스 재발행 간격(라우팅되지 않음·거부: 1초·2배·최대 60초). NULL이면 바로
ALTER TABLE flow_outboxes ADD COLUMN next_attempt_at timestamptz;

-- FLW-05.05 p95: 플로우 단위 행(node_id='*')의 실행 시간 분포(경계 1,2,5,10,20,50,100,200,500,1000,2000,5000ms, 그 이상)
ALTER TABLE flow_metric_minutes ADD COLUMN latency_buckets integer[] NOT NULL DEFAULT '{}';

-- FLW-08.04 일시 정지 BUFFER(PAUSED), FLW-05.07 parallel 대기(QUEUED)
ALTER TABLE flow_paused_triggers ADD COLUMN reason varchar(8) NOT NULL DEFAULT 'PAUSED';
ALTER TABLE flow_paused_triggers ADD CONSTRAINT ck_flow_paused_triggers_reason CHECK (reason IN ('PAUSED','QUEUED'));

-- FLW-03.04 실행 추적(API-FLW-41): 디버그를 켠 노드를 지났거나 샘플된 메시지만 1시간 보관. 인스턴스가 여럿이어도 어느 인스턴스로 물어도 보인다
CREATE TABLE flow_traces (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    flow_id          uuid          NOT NULL,
    message_id       varchar(64)   NOT NULL,
    flow_version     integer       NOT NULL,
    trace            jsonb         NOT NULL,
    created_at       timestamptz   NOT NULL DEFAULT now(),
    expires_at       timestamptz   NOT NULL,
    CONSTRAINT pk_flow_traces PRIMARY KEY (id)
);
CREATE INDEX ix_flow_traces_message ON flow_traces (message_id, flow_id);
CREATE INDEX ix_flow_traces_expires ON flow_traces (expires_at);
COMMENT ON TABLE flow_traces IS '실행 추적(FLW-03.04). 디버그 노드를 지났거나 샘플된 메시지, 1시간 보관';

-- FLW-03.06 과거 재생(API-FLW-13): 드라이런 작업. 인스턴스 하나가 잡아 실행하고(locked_by·heartbeat_at), 멈추면 다른 인스턴스가 넘겨받는다
CREATE TABLE flow_replay_jobs (
    id               uuid          NOT NULL,
    organization_id  bigint        NOT NULL,
    flow_id          uuid          NOT NULL,
    status           varchar(10)   NOT NULL DEFAULT 'QUEUED',
    request          jsonb         NOT NULL,
    processed        bigint        NOT NULL DEFAULT 0,
    total            bigint,
    result           jsonb,
    error            varchar(500),
    locked_by        varchar(64),
    heartbeat_at     timestamptz,
    created_at       timestamptz   NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CONSTRAINT pk_flow_replay_jobs PRIMARY KEY (id),
    CONSTRAINT ck_flow_replay_jobs_status CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED'))
);
CREATE INDEX ix_flow_replay_jobs_status ON flow_replay_jobs (status, created_at) WHERE status IN ('QUEUED','RUNNING');
COMMENT ON TABLE flow_replay_jobs IS '과거 재생 작업(FLW-03.06, BR-FLW-11: 행동은 기록만, 노드 상태는 메모리). 끝난 작업 7일 보관';
