package io.hnsn.kaukus.persistence;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SSTableResult {
    public static SSTableResult EMPTY = new SSTableResult(null);
    public static SSTableResult TOMBSTONE = new SSTableResult(true);

    private String value;
    boolean isTombstone;

    public SSTableResult(String value) {
        this.value = value;
        this.isTombstone = false;
    }

    private SSTableResult(boolean isTombstone) {
        this.value = null;
        this.isTombstone = isTombstone;
    }

    public boolean hasValue() {
        return !isTombstone && value != null;
    }
}