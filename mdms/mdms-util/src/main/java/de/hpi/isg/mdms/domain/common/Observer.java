package de.hpi.isg.mdms.domain.common;

/**
 * Own slim implementation of the <a href="http://en.wikipedia.org/wiki/Observer_pattern">Observer pattern</a>. Observer
 * get notified by {@link java.util.Observable}s.
 */
public interface Observer<T> {
    /**
     * Generates random integer ids.
     * 
     * @return
     */
    public int generateRandomId();

    /**
     * This method is called by objects that want to be registered by the {@link de.hpi.isg.mdms.domain.common.Observer}.
     * 
     * @param t
     *        to register
     */
    public void registerTargetObject(T t);
}
