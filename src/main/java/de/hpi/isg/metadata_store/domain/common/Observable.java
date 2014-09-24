package de.hpi.isg.metadata_store.domain.common;

/**
 * Own slim implementation of the <a
 * href="http://en.wikipedia.org/wiki/Observer_pattern">Observer pattern</a>.
 * Observable notify their {@link Observer}s.
 */
public interface Observable {
    public void notifyListeners();
}
