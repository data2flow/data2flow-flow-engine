package net.java21.data2flow.flow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** data2flow-flow-engine: 자동화 플로우·규칙 실행, 노드 상태, 지속 타이머, 아웃박스, 라이브 리로드 */
@SpringBootApplication
public class FlowEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlowEngineApplication.class, args);
    }
}
