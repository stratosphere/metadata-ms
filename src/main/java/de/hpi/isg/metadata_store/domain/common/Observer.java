package de.hpi.isg.metadata_store.domain.common;

import java.util.Observable;

import de.hpi.isg.metadata_store.domain.Target;

/**
 * Own slim implementation of the <a href="http://en.wikipedia.org/wiki/Observer_pattern">Observer pattern</a>. Observer
 * get notified by {@link Observable}s.
 */
public interface Observer {
    public int generateRandomId();

//    public void registerId(int id);

    public void registerTargetObject(Target target);
}
