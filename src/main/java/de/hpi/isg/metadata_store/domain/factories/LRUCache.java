package de.hpi.isg.metadata_store.domain.factories;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    /**
     * A cache with least-recently-used strategy.
     */
    private static final long serialVersionUID = -4065480724108252213L;
    private final int maxEntries;

    public LRUCache(final int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return super.size() > maxEntries;
    }
}