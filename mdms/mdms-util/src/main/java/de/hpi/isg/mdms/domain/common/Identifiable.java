package de.hpi.isg.mdms.domain.common;

/**
 * Everything that shall be unambiguously identified within a certain scope by a numeric ID is
 * {@link de.hpi.isg.mdms.domain.common.Identifiable}.
 *
 */
public interface Identifiable {
    /**
     * Returns the id of an {@link de.hpi.isg.mdms.domain.common.Identifiable} object.
     * 
     * @return the id
     */
    public int getId();
}
