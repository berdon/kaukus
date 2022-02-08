package io.hnsn.kaukus.node;

public interface OnUnrecoverableErrorListener {
    void onError(String message, Throwable throwable);
}
