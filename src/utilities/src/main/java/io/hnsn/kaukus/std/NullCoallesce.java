package io.hnsn.kaukus.std;

public final class NullCoallesce {
    @SafeVarargs
    public static <TValue> TValue of(TValue... values) {
        for (TValue value : values) if (value != null) return value;
        return null;
    }
}
