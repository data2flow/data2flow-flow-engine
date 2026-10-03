package net.java21.data2flow.flow.node.service;

import net.java21.data2flow.contracts.flow.FlowNodeType;
import net.java21.data2flow.flow.plan.domain.Jsons;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/** 노드 카탈로그 항목은 {@code classpath:node-types/{type}.json}(flow-node-type.v1.json 모양)에 둔다 */
final class NodeDescriptors {

    private NodeDescriptors() {
    }

    static FlowNodeType load(String type) {
        try (InputStream in = NodeDescriptors.class.getResourceAsStream("/node-types/" + type + ".json")) {
            if (in == null) {
                throw new IllegalStateException("노드 카탈로그 항목이 없습니다: " + type);
            }
            return Jsons.MAPPER.readValue(in, FlowNodeType.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
