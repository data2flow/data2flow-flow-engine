# data2flow-flow-engine

자동화 플로우 실행 엔진입니다. `data2flow.telemetry`를 받아 배포된 플로우를 실행하고, 노드 상태·지속 타이머를 PostgreSQL에 두며, 기기 제어 같은 행동은 아웃박스를 거쳐 `data2flow.actions`로 보냅니다. 플로우 정의·버전은 core-api가 갖고 이 서비스는 실행 상태만 갖습니다.

- 관련 스펙: FLW-05.01·05.02·05.03(M3), FLW-01.06·06.01의 엔진 쪽(정본은 비공개 저장소 `data2flow-docs`: `design/flow-engine-and-live-reload.md`, `design/api/FLW-api.md` §5·§8, `design/erd/flow.md`)
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

## 메시징·내부 API

| 방향 | 이름 |
|---|---|
| 소비 | Super Stream `data2flow.telemetry`(그룹 `flow`), fanout `data2flow.config`(인스턴스 임시 큐 `flow.config.*`) |
| 발행 | direct `data2flow.actions` 라우팅 키 `command`(큐 `action.commands`도 같은 인자로 선언), topic `data2flow.events` `flow.apply.reported`, topic `data2flow.debug` `flow.{flowId}`(손실 허용) |
| 부르는 core API | API-FLW-80 `GET /internal/core/flows/runtime?sinceVersion=`, API-FLW-81 `GET /internal/core/flows/{flow-id}/runtime`, API-DEV-128 `GET /internal/core/spaces/{space-id}/devices?relation=measures` |
| 여는 내부 API | API-FLW-82 `GET /internal/flow/flows/{flow-id}/apply-status`, API-FLW-83 `GET /internal/flow/node-types`, API-FLW-84 `POST /internal/flow/definitions/validate`. 플로우 지표(API-FLW-14 `…/metrics`)는 FLW-05.05(M4)에서 연다 — 그 전까지 core가 503 `FLOW_METRICS_UNAVAILABLE` |

## 빌드와 실행

```bash
./mvnw verify                 # 단위·통합 테스트(Testcontainers PostgreSQL 18·RabbitMQ 3.13) + 커버리지 80% 검사
./mvnw spring-boot:run        # 로컬 실행(프로필 local)
```

- `TimerFailoverIT`은 엔진을 별도 JVM 두 개로 띄워 kill -9 하는 M3 완료 확인 시험입니다(약 1분 30초).
- **로컬 실행은 기본으로 실행 기능을 끕니다**(`DATA2FLOW_FLOW_RUNTIME_ENABLED=false`): DB `data2flow`를 운영과 함께 쓰므로 로컬이 운영 타이머를 발화하거나 운영 아웃박스를 dev vhost로 보내면 안 됩니다. 켜야 하면 별도 조직의 시뮬레이터 데이터로만, action은 끄거나 드라이런으로 둡니다. 소비자 그룹은 `flow-<DATA2FLOW_DEV_NAME>`입니다.
- 공통 라이브러리 `data2flow-contracts`는 GitHub Packages에 있어서 읽기에도 토큰이 필요합니다. `~/.m2/settings.xml`에 서버 `github`를 넣거나 `data2flow-contracts`를 받아 `./mvnw install` 합니다.

## 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
