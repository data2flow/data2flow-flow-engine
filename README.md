# data2flow-flow-engine

자동화 플로우 실행 엔진입니다. `data2flow.telemetry`를 받아 배포된 플로우를 실행하고, 노드 상태·지속 타이머를 PostgreSQL에 두며, 기기 제어 같은 행동은 아웃박스를 거쳐 `data2flow.actions`로 보냅니다. 플로우 정의·버전은 core-api가 갖고 이 서비스는 실행 상태만 갖습니다.

- 관련 스펙: FLW-05.01·05.02·05.03(M3), M4 엔진 쪽 FLW-03.01~03.06·05.04·05.05·05.07·06.01~06.04·06.07·08.01~08.04·10.02·10.03, RUL-01.01~01.07·01.12(규칙 → 플로우 컴파일), FLW-01.06·06.01의 엔진 쪽(정본은 비공개 저장소 `data2flow-docs`: `design/flow-engine-and-live-reload.md`, `design/api/FLW-api.md` §5·§8, `design/erd/flow.md`)
- 패키지: `net.java21.data2flow.flow` · Spring Boot 4.1.1 · Java 21 · Maven Wrapper
- 포트: API 8080(내부 API만, 외부 경로 없음), actuator 8081(프로브·지표 전용)

## M3에서 하는 일

| 영역 | 내용 |
|---|---|
| 불변 실행 계획 | 정의(`FlowDefinition`) → 컴파일(노드 설정·포트 타입·순환·트리거 검사, 오류는 `{field, code, message}`) → `AtomicReference`로 원자적 전환, 메시지는 시작할 때 읽은 계획 하나로(BR-FLW-06), 이전 계획은 참조 수 0이면 드레인 |
| 노드(9종) | `trigger.telemetry`, `condition.threshold`(지속 시간·히스테리시스), `condition.switch`, `transform.map`, `transform.aggregate`, `transform.js`(GraalJS 커뮤니티판 샌드박스, 공용 모듈 `data2flow-script-sandbox`·ADR-046), `flow.delay`, `action.control`, `debug.log`. 카탈로그는 `src/main/resources/node-types/*.json` |
| 상태·타이머 | `flow_node_state`(대상 키당 256KB), `flow_timers`(만기 후보 → 상태 잠금 → `FOR UPDATE SKIP LOCKED` → 발화 → FIRED), `flow_partition_progress`(다시 읽은 메시지 건너뛰기) |
| 행동 | `flow_outboxes`(멱등 키 `sha256(flowId, nodeId, triggerMessageId)`, 버전 없음, BR-FLW-13) → 릴레이가 publisher confirm 뒤 `sent_at`. 출처 `source.spaceId`는 트리거 메시지의 공간(없으면 공간 대상)이고 action이 기기 대상 명령의 샌드박스 판정(BR-ACT-23)에 쓴다 |
| 오류 격리 | 노드 예외는 error 포트(와이어가 없으면 그 갈래만 끝), 플로우마다 따로 커밋, DB 장애만 재시도 |
| 라이브 리로드 | `data2flow.config`(FLOW·OVERLAY) 수신 → core API-FLW-81로 다시 읽어 적용, 30초마다 API-FLW-80 폴링, 적용 결과는 `flow_instance_versions` + `flow.apply.reported` |
| 다중 인스턴스 | Super Stream Single Active Consumer(그룹 `flow`, 로컬 `flow-<개발자>`), 파티션별 순차 디스패처, 타이머는 행 잠금으로 한 번만 |

## M4에서 더한 것(자동화 완성)

| 영역 | 내용 |
|---|---|
| 라이브 리로드 | 메시지별 계획 고정(`FlowRegistry.pin`: 참조 수 −1 = 닫힘, 닫힌 계획은 잡지 않고 새 계획을 다시 읽음), 참조 카운트 드레인, `data2flow.config` FLOW·OVERLAY 수신 즉시 다시 읽기. **부하 시험 `LiveReloadIT`(TC-FLW-134)**: 초당 200건 60초 중 v12→v22 10회 적용 → 메시지 12,000건 모두 버전 1개로 처리, 유실·중복 0, 감지→제어(publisher confirm) p95 약 0.35초 |
| 상태 이어받기 | KEEP/RESET/MIGRATE(FLW-06.03): 상태 행에 쓴 노드의 지문(`state_config` = 종류 + 정책이 KEEP이 아닌 설정 값)을 남기고, 읽을 때 지금 노드와 다르면 카탈로그 `statePolicy`로 정책을 적용(집계 창은 MIGRATE로 표본 자름). RESET 노드는 적용 직후 이전 지문의 행·판정 타이머 정리. 삭제 노드 24시간 보관·롤백 복원 |
| 처리량 | 파티션 작업자가 밀린 메시지를 최대 50건 묶어 플로우당 트랜잭션 하나로 처리(메시지마다 버전 1개 유지), 처리 진행은 마지막 문장 하나(`advance`), 아웃박스 릴레이는 묶음을 먼저 보내고 확인을 모아 기다림(파이프라이닝)·일괄 sent·재발행 간격(1초·2배·최대 60초) |
| 실행 모드·일시 정지 | single(진행 중이면 버림)·restart(진행 중 타이머 취소)·parallel(max, 넘으면 QUEUED 보관 뒤 순서대로), queued는 M3 동작(파티션 순서). 일시 정지 DROP(버린 트리거 지표)·BUFFER(1시간 보관, 재개 뒤 받은 순서) |
| 안전장치 | 초당 실행 한도(기본 100, 버전 값)·순환(`meta.lineage`) → 그 인스턴스에서 PAUSED + EVT-FLW-03(RUNAWAY·CYCLE), core가 PAUSED를 알려 줄 때까지 유지. DEGRADED(5분 100건 이상·오류율 10%)·RECOVERED(10분 연속 미만). 대기 타이머 10,000(`FLOW_TIMER_LIMIT`), 분기 100 초과 시 그 실행 중단(`FLOW_FANOUT_LIMIT`) |
| 오류·재시도 | 노드 재시도(일반 0, 행동 3회, 1초·2배·최대 30초)를 RETRY 타이머로 미룸(다음 메시지 안 막음), 마지막 실패는 error 포트 `{…, error:{nodeId, errorType, message, attempts}}` |
| 비상 정지·유지보수 | EVT-ACT-03·EVT-OPS-02(인스턴스 임시 큐 `flow.guard.*`)·`data2flow.config` EMERGENCY_STOP·주기 동기화로 범위를 알고, 범위 안 제어·장면은 `skipped(EMERGENCY_STOP·MAINTENANCE)` 기록(BR-FLW-19) |
| 제어 결과 | `action.control`의 ok·failed에 연결선이 있으면 `awaitResult=true` + 결과 대기 타이머(AWAIT_RESULT). EVT-ACT-01 끝 상태(큐 `flow.events`)가 오면 APPLIED → ok, 나머지 → failed, 만기면 failed(TIMEOUT) |
| 노드(16종) | M3 9종 + `sink.database`(SinkWriteRequest, 배치 100·UPSERT 같은 키 1행) · `action.notify`(NotificationRequest) · `action.alarm`(alarm.signal, 키 `flow:`/`rule:`) · `condition.noData`(timeout·restored) · `condition.rateOfChange` · `condition.timeWindow` · `condition.group`. 임계값 `repeat`, 트리거 `tags`, JS `scriptRef` |
| 라이브 뷰·추적·지표 | `node.stats`(1초, 배지 OK·WARN·ERROR)·`node.sample`(노드당 5/초, 디버그 50/초) → `FlowDebugMessage` v1, 추적은 디버그 노드를 지났거나 플로우당 초당 5건만 `flow_traces` 1시간, 지표는 `flow_metric_minutes`(플로우 행 `*`: 실행·오류 실행·지연 분포·행동·버린 트리거) |
| 드라이런 | 시험 실행(메시지 하나, 타이머는 가상 시각으로 최대 24시간·100개), 과거 재생(7일 이하, core 과거 텔레메트리 쪽 단위, 작업 `flow_replay_jobs`) — 행동은 기록만, 운영 상태 변경 없음(BR-FLW-11) |
| 규칙 컴파일 | 규칙(API-RUL-02 모양) → 표준 플로우(트리거 → [공간 집계] → 조건 → [시간] → 알람 RAISE·CLEAR). 노드 ID 역할별 고정. 템플릿 7종 × 7일 참조 평가기 동등성 시험(TC-RUL-003) |

## 메시징·내부 API

| 방향 | 이름 |
|---|---|
| 소비 | Super Stream `data2flow.telemetry`(그룹 `flow`), fanout `data2flow.config`(인스턴스 임시 큐 `flow.config.*`), topic `data2flow.events`의 `command.status.*`(Quorum 큐 `flow.events`), `control.emergency.*`·`ops.maintenance.*`(인스턴스 임시 큐 `flow.guard.*`) |
| 발행 | direct `data2flow.actions` 라우팅 키 `command`·`notify`·`sink`(큐 `action.commands`·`action.notifications`·`action.sinks`와 DLQ를 같은 인자로 선언), topic `data2flow.events` `flow.apply.reported`·`flow.state.changed`·`alarm.signal`(아웃박스), topic `data2flow.debug` `flow.{flowId}`(손실 허용) |
| 부르는 core API | API-FLW-80·81, API-DEV-128(측정 기기·관계 무관 기기), API-ACT-46 `GET /internal/core/emergency-stops?active=true`, API-OPS-24 `GET /internal/core/maintenance-windows?status=ACTIVE`, API-SCR-32 `GET /internal/core/scripts/runtime-bundle`(scriptRef), API-DEV-122 `GET /internal/core/devices/{device-id}/runtime`의 `tags[]`(트리거 태그), API-FLW-87 `GET /internal/core/telemetry/history`(과거 재생). 경로가 없으면(404) 없음으로 본다 |
| 여는 내부 API | API-FLW-82 apply-status, API-FLW-83 node-types, API-FLW-84 definitions/validate, **API-FLW-14 `GET /internal/flow/flows/{flow-id}/metrics?window=&step=`**(core는 503 대신 이 응답을 중계), API-FLW-41 `GET /internal/flow/traces/{message-id}?flowId=`, API-FLW-12 `POST /internal/flow/test-runs`, API-FLW-13 `POST /internal/flow/replays`·`GET /internal/flow/replays/{job-id}`·`POST …/{job-id}/cancel`, API-FLW-86 `POST /internal/flow/rules/compile` |

## 빌드와 실행

```bash
./mvnw verify                 # 단위·통합 테스트(Testcontainers PostgreSQL 18·RabbitMQ 3.13) + 커버리지 80% 검사
./mvnw spring-boot:run        # 로컬 실행(프로필 local)
```

- `TimerFailoverIT`은 엔진을 별도 JVM 두 개로 띄워 kill -9 하는 M3 완료 확인 시험입니다(약 1분 30초).
- `LiveReloadIT`는 M4 완료 확인 부하 시험입니다(예열 5초 + 초당 200건 60초, 약 2분 30초). 결과 줄 `[TC-FLW-134]`에 버전별 처리 수와 감지→제어 p50·p95·p99를 찍습니다.
- **로컬 실행은 기본으로 실행 기능을 끕니다**(`DATA2FLOW_FLOW_RUNTIME_ENABLED=false`): DB `data2flow`를 운영과 함께 쓰므로 로컬이 운영 타이머를 발화하거나 운영 아웃박스를 dev vhost로 보내면 안 됩니다. 켜야 하면 별도 조직의 시뮬레이터 데이터로만, action은 끄거나 드라이런으로 둡니다. 소비자 그룹은 `flow-<DATA2FLOW_DEV_NAME>`입니다.
- 공통 라이브러리 `data2flow-contracts`는 GitHub Packages에 있어서 읽기에도 토큰이 필요합니다. `~/.m2/settings.xml`에 서버 `github`를 넣거나 `data2flow-contracts`를 받아 `./mvnw install` 합니다.

## 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
