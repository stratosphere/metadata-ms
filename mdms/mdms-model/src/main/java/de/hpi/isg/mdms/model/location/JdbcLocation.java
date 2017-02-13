package de.hpi.isg.mdms.model.location;

import java.util.Collection;
import java.util.LinkedList;

/**
 * This standard {@link Location} implementation describes a database via a JDBC URL.
 */
public class JdbcLocation extends DefaultLocation {

    /**
     * Configuration key for the JDBC URL.
     */
    public static final String URL = "URL";

    /**
     * Configuration key for the JDBC driver class.
     */
    public static final String DRIVER_CLASS = "DRIVER";

    /**
     * Configuration key for the schema name.
     */
    public static final String SCHEMA = "SCHEMA";

    public void setUrl(String url) {
        this.set(URL, url);
    }

    public String getUrl() {
        return this.get(URL);
    }

    public void setSchema(String schema) {
        this.set(SCHEMA, schema);
    }

    public String getSchema() {
        return this.get(SCHEMA);
    }

    public void setDriverClass(String driverClass) {
        this.set(DRIVER_CLASS, driverClass);
    }

    public String getDriverClass() {
        return this.get(DRIVER_CLASS);
    }

    @Override
    public Collection<String> getAllPropertyKeys() {
        LinkedList<String> allPropertyKeys = new LinkedList<>(super.getAllPropertyKeys());
        allPropertyKeys.add(URL);
        allPropertyKeys.add(DRIVER_CLASS);
        return allPropertyKeys;
    }

    @Override
    public Collection<String> getPropertyKeysForValueCanonicalization() {
        LinkedList<String> propertyKeys = new LinkedList<>(super.getPropertyKeysForValueCanonicalization());
        propertyKeys.add(URL);
        propertyKeys.add(DRIVER_CLASS);
        return propertyKeys;
    }


}
