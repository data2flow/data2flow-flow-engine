-- data2flow_flow 초기 스키마(FLW-05.01·05.02·05.03, design/erd/flow.md, 초안 design/erd/ddl/21-flow.sql).
-- 소유: data2flow-flow-engine. 플로우 정의는 core-api(data2flow_core)에 있고 여기에는 실행 상태만 둔다. 테이블 사이 FK 없음.
-- Flyway는 schemas=data2flow_flow로 실행되므로 스키마 이름을 쓰지 않는다.
CREATE TABLE flow_node_state (
    flow_id               uuid          NOT NULL,
    node_id               varchar(64)   NOT NULL,
    target_key            varchar(128)  NOT NULL,
    organization_id       bigint        NOT NULL,
    state                 jsonb         NOT NULL DEFAULT '{}'::jsonb,
    state_policy_version  integer       NOT NULL DEFAULT 1,
    retain_until          timestamptz,
    updated_at            timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_flow_node_state PRIMARY KEY (flow_id, node_id, target_key),
    CONSTRAINT ck_flow_node_state_size CHECK (pg_column_size(state) <= 262144)
);
CREATE INDEX ix_flow_node_state_retain ON flow_node_state (retain_until) WHERE retain_until IS NOT NULL;
CREATE INDEX ix_flow_node_state_org_flow ON flow_node_state (organization_id, flow_id);
COMMENT ON TABLE flow_node_state IS '노드 상태. 메시지 처리 트랜잭션에서 아웃박스·처리 진행과 함께 저장(BR-FLW-29). 삭제 노드는 retain_until(24시간) 보관(BR-FLW-07)';

CREATE TABLE flow_timers (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    flow_id          uuid          NOT NULL,
    flow_version     integer       NOT NULL,
    node_id          varchar(64)   NOT NULL,
    target_key       varchar(128)  NOT NULL,
    kind             varchar(12)   NOT NULL,
    due_at           timestamptz   NOT NULL,
    context          jsonb         NOT NULL DEFAULT '{}'::jsonb,
    status           varchar(10)   NOT NULL DEFAULT 'WAITING',
    locked_until     timestamptz,
    attempts         smallint      NOT NULL DEFAULT 0,
    created_at       timestamptz   NOT NULL DEFAULT now(),
    fired_at         timestamptz,
    CONSTRAINT pk_flow_timers PRIMARY KEY (id),
    CONSTRAINT ck_flow_timers_kind CHECK (kind IN ('DELAY','WAIT_UNTIL','RECHECK','APPROVAL')),
    CONSTRAINT ck_flow_timers_status CHECK (status IN ('WAITING','FIRED','CANCELLED'))
);
-- 엔진: SELECT ... WHERE status='WAITING' AND due_at <= now() ORDER BY due_at LIMIT n FOR UPDATE SKIP LOCKED
CREATE INDEX ix_flow_timers_due_waiting ON flow_timers (due_at) WHERE status = 'WAITING';
CREATE INDEX ix_flow_timers_flow_node_target ON flow_timers (flow_id, node_id, target_key) WHERE status = 'WAITING';
COMMENT ON TABLE flow_timers IS '지속 타이머. 여러 인스턴스가 SKIP LOCKED로 가져가 한 번만 발화. 플로우당 대기 10,000개(BR-FLW-16)';

CREATE TABLE flow_outboxes (
    id                  bigint GENERATED ALWAYS AS IDENTITY,
    organization_id     bigint        NOT NULL,
    idempotency_key     char(64)      NOT NULL,
    kind                varchar(12)   NOT NULL,
    exchange            varchar(64)   NOT NULL DEFAULT 'data2flow.actions',
    routing_key         varchar(128)  NOT NULL,
    payload             jsonb         NOT NULL,
    flow_id             uuid          NOT NULL,
    flow_version        integer       NOT NULL,
    node_id             varchar(64)   NOT NULL,
    trigger_message_id  varchar(64)   NOT NULL,
    created_at          timestamptz   NOT NULL DEFAULT now(),
    sent_at             timestamptz,
    attempts            smallint      NOT NULL DEFAULT 0,
    last_error          varchar(500),
    CONSTRAINT pk_flow_outboxes PRIMARY KEY (id),
    CONSTRAINT uq_flow_outboxes_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_flow_outboxes_kind CHECK (kind IN ('COMMAND','NOTIFY','SINK','SCENE','WORK_ORDER','ANALYSIS','EVENT'))
);
-- 릴레이: SELECT ... WHERE sent_at IS NULL ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED
CREATE INDEX ix_flow_outboxes_unsent ON flow_outboxes (created_at) WHERE sent_at IS NULL;
COMMENT ON COLUMN flow_outboxes.idempotency_key IS 'sha256(flowId, nodeId, triggerMessageId[, 분할 인덱스]). 버전 미포함(BR-FLW-13)';

CREATE TABLE flow_paused_triggers (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    flow_id          uuid          NOT NULL,
    target_key       varchar(128)  NOT NULL,
    message          jsonb         NOT NULL,
    received_at      timestamptz   NOT NULL,
    CONSTRAINT pk_flow_paused_triggers PRIMARY KEY (id)
);
CREATE INDEX ix_flow_paused_triggers_flow_target_received ON flow_paused_triggers (flow_id, target_key, received_at);
COMMENT ON TABLE flow_paused_triggers IS '일시 정지 BUFFER 모드의 트리거. 1시간 보관(BR-FLW-25)';

CREATE TABLE flow_metric_minutes (
    flow_id          uuid         NOT NULL,
    node_id          varchar(64)  NOT NULL,
    minute           timestamptz  NOT NULL,
    organization_id  bigint       NOT NULL,
    processed        integer      NOT NULL DEFAULT 0,
    errors           integer      NOT NULL DEFAULT 0,
    dropped          integer      NOT NULL DEFAULT 0,
    total_ms         bigint       NOT NULL DEFAULT 0,
    actions_command  integer      NOT NULL DEFAULT 0,
    actions_notify   integer      NOT NULL DEFAULT 0,
    actions_sink     integer      NOT NULL DEFAULT 0,
    CONSTRAINT pk_flow_metric_minutes PRIMARY KEY (flow_id, node_id, minute)
);
CREATE INDEX ix_flow_metric_minutes_org_minute ON flow_metric_minutes (organization_id, minute);

CREATE TABLE flow_instance_versions (
    instance_id       varchar(64)  NOT NULL,
    flow_id           uuid         NOT NULL,
    organization_id   bigint       NOT NULL,
    applied_version   integer      NOT NULL,
    overlay_revision  integer      NOT NULL DEFAULT 0,
    reported_at       timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT pk_flow_instance_versions PRIMARY KEY (instance_id, flow_id)
);
CREATE INDEX ix_flow_instance_versions_flow ON flow_instance_versions (flow_id);

CREATE TABLE flow_partition_progress (
    flow_id           uuid         NOT NULL,
    stream_partition  smallint     NOT NULL,
    organization_id   bigint       NOT NULL,
    watermark_offset  bigint       NOT NULL DEFAULT -1,
    processed_above   bigint[]     NOT NULL DEFAULT '{}',
    updated_at        timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT pk_flow_partition_progress PRIMARY KEY (flow_id, stream_partition),
    CONSTRAINT ck_flow_partition_progress_partition CHECK (stream_partition BETWEEN 0 AND 11),
    CONSTRAINT ck_flow_partition_progress_above CHECK (cardinality(processed_above) <= 10000)
);
COMMENT ON TABLE flow_partition_progress IS '(플로우, 스트림 파티션)별 처리 진행. watermark 이하이거나 processed_above에 있으면 재처리 시 건너뜀(BR-FLW-29)';

