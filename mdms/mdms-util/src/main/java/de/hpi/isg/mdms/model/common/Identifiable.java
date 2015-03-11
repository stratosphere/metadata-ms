package de.hpi.isg.mdms.model.common;

/**
 * Everything that shall be unambiguously identified within a certain scope by a numeric ID is
 * {@link Identifiable}.
 *
 */
public interface Identifiable {
    /**
     * Returns the id of an {@link Identifiable} object.
     * 
     * @return the id
     */
    public int getId();
}
