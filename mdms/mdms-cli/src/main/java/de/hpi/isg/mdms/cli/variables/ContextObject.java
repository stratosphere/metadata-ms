package de.hpi.isg.mdms.cli.variables;

import de.hpi.isg.mdms.cli.SessionContext;

import java.io.Serializable;

/**
 * An object that resides in the context of a {@link SessionContext}.
 */
public interface ContextObject extends Serializable {

    /**
     * Represents a {@code null} value.
     */
    ContextValue<Void> NULL = new ContextValue<Void>() {

        @Override
        public String toParseableString() {
            return "null";
        }

        @Override
        public String toReadableString() {
            return "null";
        }

        @Override
        public boolean isException() {
            return false;
        }

        @Override
        public String toString() {
            return "Null";
        }

        @Override
        public Void getValue() {
            return null;
        }
    };


    /**
     * @return {@link String} representation that can be parsed
     */
    String toParseableString();

    /**
     * @return a human-readable {@link String} representation
     */
    String toReadableString();

    /**
     * @return whether this object represents a value
     */
    boolean isValue();

    /**
     * @return whether this object represents a reference
     */
    boolean isReference();

    /**
     * @return whether this object represents an exception
     */
    boolean isException();

}
