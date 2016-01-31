package de.hpi.isg.mdms.cli.variables;

/**
 * {@link ContextObject} representation of some value.
 */
public abstract class ContextValue<T> implements ContextObject {

    public abstract T getValue();

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public boolean isReference() {
        return false;
    }

    @Override
    public boolean isException() {
        return false;
    }
}
