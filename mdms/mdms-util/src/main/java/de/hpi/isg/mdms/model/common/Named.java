package de.hpi.isg.mdms.model.common;

/**
 * Everything that shall be named for easier understandability is {@link Named}. Names do
 * not have to be unique.
 *
 */
public interface Named {
    /**
     * Returns the name of a {@link Named} object.
     * 
     * @return the name
     */
    public String getName();
}
