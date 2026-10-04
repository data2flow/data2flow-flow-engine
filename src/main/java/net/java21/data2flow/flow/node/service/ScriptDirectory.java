package net.java21.data2flow.flow.node.service;

import java.util.Optional;

/**
 * JS 함수 노드의 스크립트 참조({@code scriptRef: s-{id}@v{n}}, FLW-02·SCR-01)를 코드로 바꾼다. 원천은 core API-SCR-32 실행 묶음(활성 버전만)이다.
 */
public interface ScriptDirectory {

    /**
     * 스크립트 코드.
     *
     * @return 그 버전이 활성 버전이면 코드. 스크립트가 없거나 다른 버전이 활성이면 빈 값
     */
    Optional<String> code(long organizationId, long scriptId, int versionNo);

    /** 모르는 디렉터리(시험·드라이런) */
    ScriptDirectory NONE = (organizationId, scriptId, versionNo) -> Optional.empty();
}
