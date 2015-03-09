package de.hpi.isg.mdms.domain.common;

/**
 * Everything that shall be named for easier understandability is {@link de.hpi.isg.mdms.domain.common.Named}. Names do
 * not have to be unique.
 *
 */
public interface Named {
    /**
     * Returns the name of a {@link de.hpi.isg.mdms.domain.common.Named} object.
     * 
     * @return the name
     */
    public String getName();
}
