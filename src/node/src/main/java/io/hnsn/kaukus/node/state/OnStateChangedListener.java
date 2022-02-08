package io.hnsn.kaukus.node.state;

public interface OnStateChangedListener {
    void onChanged(NodeState source, NodeState destination);
}
