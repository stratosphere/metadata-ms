package de.hpi.isg.mdms.cli.variables;


import de.hpi.isg.mdms.cli.exceptions.CliException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Namespace} maps variable names to {@link ContextObject}s. This implies that {@link Namespace}s can be
 * nested.
 */
public class Namespace extends ContextValue<Map<String, ContextObject>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Namespace.class);

    private Map<String, ContextObject> variables = new HashMap<>();

    /**
     * Associates a variable with a key.
     *
     * @param name is the key
     * @param var  is the variable
     * @return the formerly associated value or {@link ContextValue#NULL} if no such value exists
     */
    public ContextObject register(String name, ContextObject var) {
        final ContextObject oldVar = this.variables.put(name, var);
        if (oldVar != null) {
            LOGGER.debug("Disassociating {} from name {}.", var, name);
        }
        return ContextObjects.escapeNull(oldVar);
    }

    /**
     * Disassociates the variable with the given {@code name}.
     *
     * @param name the key of the association to be removed
     * @return the associated value or {@link ContextValue#NULL} if no such value exists
     */
    public ContextObject delete(String name) {
        return ContextObjects.escapeNull(this.variables.remove(name));
    }


    /**
     * Retrieves the variable with the given name from this namespace.
     *
     * @param name name of the variable within this namespace
     * @return the variable or {@link ContextObject#NULL} if no such variable exists
     */
    public ContextObject get(String name) {
        return ContextObjects.escapeNull(variables.get(name));

    }

    @Override
    public String toParseableString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        String separator = "";
        for (Map.Entry<String, ContextObject> entry : variables.entrySet()) {
            sb.append(separator).append('"').append(entry.getKey()).append("\":").append(entry.getValue().toParseableString());
            separator = ",";
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toReadableString() {
        return toParseableString();
    }

    @Override
    public Map<String, ContextObject> getValue() {
        return Collections.unmodifiableMap(this.variables);
    }

    /**
     * Deletes
     *
     * @param reference
     * @return
     * @throws CliException
     */
    public ContextObject delete(ContextReference reference) throws CliException {
        return getEnclosingNamespace(reference).delete(reference.getLastSegment());
    }


    /**
     * Returns the enclosing {@link Namespace} of the element addressed by the given {@link ContextReference}.
     *
     * @param reference designates the element whose enclosing {@link Namespace} is requested
     * @return the enclosing {@link Namespace}
     * @throws CliException if the requested {@code Namespace} does not exist
     */
    public Namespace getEnclosingNamespace(ContextReference reference) throws CliException {
        return descend(reference, 0);
    }

    /**
     * Follows the given {@code ContextReference} until its second-to-last element.
     *
     * @param reference      {@code ContextReference} that should be followed
     * @param referenceIndex index of the next reference segment to follow
     * @return the second-to-last element of the {@code ContextReference}
     * @throws CliException if the given {@code ContextReference} does not point to an existing {@code Namespace}
     */
    private Namespace descend(ContextReference reference, int referenceIndex) throws CliException {
        if (reference.getPath().size() == referenceIndex + 1) { // If last path element...
            return this;
        } else {
            final String pathElement = reference.getPath().get(referenceIndex);
            final ContextObject child = get(pathElement);
            if (!(child instanceof Namespace)) {
                throw new CliException(String.format("Could not descend %s at %d.", reference, referenceIndex));
            }
            return ((Namespace) child).descend(reference, referenceIndex + 1);
        }
    }

    /**
     * Sets the given {@code value} in the namespace as governed by the {@code targetRef}. Descends into
     * sub-{@code Namespace}s if necessary
     *
     * @param targetRef the {@link ContextReference} that describes where to set the value
     * @param value     the value to be set
     * @throws CliException if the {@link Namespace} that should host the value does not exist
     */
    public ContextObject set(ContextReference targetRef, ContextValue value) throws CliException {
        return getEnclosingNamespace(targetRef).register(targetRef.getLastSegment(), value);
    }

    /**
     * Gets the value associated to the {@code reference}.
     *
     * @param reference points to the value that should be retrieved
     * @return the retrieved value
     * @throws CliException if the enclosing {@link Namespace} of the referenced object does not exist
     */
    public ContextObject get(ContextReference reference) throws CliException {
        return getEnclosingNamespace(reference).get(reference.getLastSegment());
    }

    public <T> T get(String name, Class<T> expectedClass) {
        final ContextObject contextObject = get(name);
        if (expectedClass.isAssignableFrom(contextObject.getClass())) {
            return (T) contextObject;
        }
        throw new IllegalArgumentException(String.format("Not a %s: %s", expectedClass.getSimpleName(), contextObject));
    }

    @Override
    public String toString() {
        return toParseableString();
    }
}
