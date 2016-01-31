package de.hpi.isg.mdms.cli.variables;


import de.hpi.isg.mdms.cli.exceptions.CliException;

/**
 * This class offers utility methods related to {@link ContextObject}.
 */
public class ContextObjects {

    private ContextObjects() {
    }

    /**
     * Replaces {@code null} by {@link ContextObject#NULL}.
     *
     * @param obj the object that might need to be replaced
     * @return {@code obj} or {@link ContextObject#NULL} if it was null
     */
    public static ContextObject escapeNull(ContextObject obj) {
        return obj == null ? ContextObject.NULL : obj;
    }

    /**
     * Parses the given variable code and resolves the value from the given namespace.
     *
     * @param variableCode serialized variable name
     * @param namespace    hosts the variable value
     * @return the resolved variable
     * @throws CliException if the variable value could not be retrieved
     */
    public static ContextObject parseAndResolve(CharSequence variableCode, Namespace namespace) throws CliException {
        // Parse variable code and build a reference.
        assert variableCode.charAt(0) == '$';
        String[] path;
        if (variableCode.charAt(1) == '{') {
            assert variableCode.charAt(variableCode.length() - 1) == '}';
            path = variableCode.subSequence(2, variableCode.length() - 1).toString().split("\\.");
        } else {
            path = new String[]{variableCode.subSequence(1, variableCode.length()).toString()};
        }
        final ContextReference reference = new ContextReference(path);

        // Resolve the reference.
        return namespace.get(reference);
    }

    /**
     * Turns the given value into an appropriate {@link ContextValue}.
     *
     * @param o should be expressed as a {@link ContextValue}
     * @return a {@link ContextValue} expressing {@code o}
     */
    public static ContextValue<?> toContextValue(Object o) {
        if (o == null) {
            return ContextObject.NULL;
        } else if (o instanceof String) {
            return new StringValue((String) o);
        } else if (o instanceof Integer) {
            return new IntValue((Integer) o);
        }
        // TODO: Support more classes.

        // Fallback: Print whatever we find as a string.
        return new StringValue(o.toString());
    }
}
