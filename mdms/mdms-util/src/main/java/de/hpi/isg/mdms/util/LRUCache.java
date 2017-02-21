package de.hpi.isg.mdms.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    /**
     * A cache with least-recently-used strategy.
     */
    private static final long serialVersionUID = -4065480724108252213L;
    private int maxEntries;

    private boolean isEvictionEnabled = true;

    public LRUCache(final int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        this.maxEntries = maxEntries;
    }

    public void setEvictionEnabled(boolean evictionEnabled) {
        this.isEvictionEnabled = evictionEnabled;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        if (this.isEvictionEnabled) {
            return super.size() > this.maxEntries;
        } else {
            this.maxEntries = Math.max(this.maxEntries, super.size());
            return false;
        }
    }
}