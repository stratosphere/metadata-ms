package de.hpi.isg.metadata_store.domain.common;

import java.util.Observable;

import de.hpi.isg.metadata_store.domain.Target;

/**
 * Own slim implementation of the <a href="http://en.wikipedia.org/wiki/Observer_pattern">Observer pattern</a>. Observer
 * get notified by {@link Observable}s.
 */
public interface Observer {
    /**
     * Generates random integer ids.
     * 
     * @return
     */
    public int generateRandomId();

    /**
     * This method is called by {@link Target} objects that want to be registered by the {@link Observer}.
     * 
     * @param target
     *        to register
     */
    public void registerTargetObject(Target target);
}
