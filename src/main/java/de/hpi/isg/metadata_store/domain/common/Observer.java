package de.hpi.isg.metadata_store.domain.common;

/**
 * Own slim implementation of the <a
 * href="http://en.wikipedia.org/wiki/Observer_pattern">Observer pattern</a>.
 * Observer get notified by {@link Observable}s.
 */
public interface Observer {
    public void update(Object message);
}
