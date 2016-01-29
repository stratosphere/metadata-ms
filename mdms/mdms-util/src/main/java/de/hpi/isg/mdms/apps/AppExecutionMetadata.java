package de.hpi.isg.mdms.apps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides information about the results of an application.
 */
public class AppExecutionMetadata {

    private final Map<String, Object> customData = new HashMap<>();

    private boolean isProgramSuccess = false;

    private String[] parameters;



    // TODO: add more? e.g., start/end time, duration, VM configuration

    public void addCustomData(String key, Object value) {
        if (!key.matches("[a-zA-Z][a-zA-Z0-9]*")) {
            throw new IllegalArgumentException("Invalid custom data key: " + key);
        }
        this.customData.put(key, value);
    }

    public <T> T getCustomData(String key, Class<T> expectedClass) {
        final Object data = this.customData.get(key);
        if (data == null || expectedClass.isAssignableFrom(data.getClass())) {
            return (T) data;
        }
        throw new IllegalStateException(String.format("%s is not of type %s.", data, expectedClass));
    }

    public Map<String, Object> getCustomData() {
        return Collections.unmodifiableMap(this.customData);
    }

    public boolean isProgramSuccess() {
        return this.isProgramSuccess;
    }

    public String[] getParameters() {
        return this.parameters;
    }

    public void setIsProgramSuccess(boolean isProgramSuccess) {
        this.isProgramSuccess = isProgramSuccess;
    }

    public void setParameters(String[] parameters) {
        this.parameters = parameters;
    }
}
