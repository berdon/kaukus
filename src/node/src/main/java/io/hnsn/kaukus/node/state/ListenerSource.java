package io.hnsn.kaukus.node.state;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

@SuppressWarnings("unchecked")
public class ListenerSource {
    private final Map<Class<?>, CopyOnWriteArraySet<?>> listeners = new HashMap<>();

    public <TListener> CopyOnWriteArraySet<TListener> get(Class<TListener> cls) {
        if (!listeners.containsKey(cls)) {
            listeners.put(cls, new CopyOnWriteArraySet<>());
        }

        return (CopyOnWriteArraySet<TListener>) listeners.get(cls);
    }

    public void clear() {
        listeners.clear();
    }
}
