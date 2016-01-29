package de.hpi.isg.mdms.util;

/**
 * This class simply encapsulates a reference and can be used for IN/OUT parameters.
 *
 * @author sebastian.kruse
 * @since 29.04.2015
 */
public class Reference<T> {

    /**
     * Resolves the given reference or returns {@code null} if no reference is given.
     * @param reference is the reference to resolve or {@code null}
     * @param <T> is the type of the reference
     * @return the referencee or {@code null}
     */
    public static <T> T resolve(Reference<T> reference) {
        return reference == null ? null : reference.get();
    }

    private T referencee;

    /**
     * Creates a new empty reference.
     */
    public Reference() {
        this(null);
    }

    /**
     * Creates a new reference.
     * @param referencee will be referenced
     */
    public Reference(T referencee) {
        this.referencee = referencee;
    }

    /**
     * Sets the referencee.
     */
    public void set(T referencee) {
        this.referencee = referencee;
    }

    /**
     * @return the referencee
     */
    public T get() {
        return this.referencee;
    }

}
