package io.hnsn.kaukus.directory;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NodeIdentity {
    private final String identifier;
    private final String address;
    private final int port;
    private final NodeStatus status;
}
