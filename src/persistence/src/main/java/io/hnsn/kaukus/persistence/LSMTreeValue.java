package io.hnsn.kaukus.persistence;

import lombok.Getter;

@Getter
public class LSMTreeValue {
    public static final LSMTreeValue TOMBSTONE = new LSMTreeValue(true);

    private final String value;
    private final boolean isTombstone;

    public <T> LSMTreeValue(String value) {
        this.value = value;
        this.isTombstone = false;
    }

    private LSMTreeValue(boolean isTombstone) {
        this.value = null;
        this.isTombstone = isTombstone;
    }
}