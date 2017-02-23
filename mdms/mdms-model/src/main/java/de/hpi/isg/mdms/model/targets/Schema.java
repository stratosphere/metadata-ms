package de.hpi.isg.mdms.model.targets;

import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.Location;

import java.util.Collection;

/**
 * A {@link Schema} combines multiple corresponding {@link Table}s.
 */

public interface Schema extends Target {

    /**
     * Adds a new table with the given name to this schema.
     *
     * @param metadataStore is the metadata store in which the new table shall be stored
     * @param name          is the name of the table
     * @param location      is the location of the table
     * @return the added table
     */
    Table addTable(MetadataStore metadataStore, String name, String description, Location location);

    Table getTableByName(String name) throws NameAmbigousException;

    Collection<Table> getTablesByName(String name);

    Table getTableById(int id);

    Collection<Table> getTables();

    /**
     * Resolves a {@link Target} by its name.
     *
     * @param targetName the name of the {@link Target} conforming to the regex {@code <table> ( "." <column>)?}
     * @return the resolved {@link Target} or {@code null} if it could not be resolved
     * @throws NameAmbigousException if more than one {@link Target} matches
     */
    default Target getTargetByName(String targetName) throws NameAmbigousException {
        Target lastEncounteredTarget = null;
        int separatorIndex = -1;
        do {
            separatorIndex = targetName.indexOf('.', separatorIndex + 1);
            if (separatorIndex == -1) separatorIndex = targetName.length();
            String tableName = targetName.substring(0, separatorIndex);
            Table table = this.getTableByName(tableName);
            if (table != null) {
                if (separatorIndex == targetName.length()) {
                    if (lastEncounteredTarget != null) throw new NameAmbigousException(targetName);
                    lastEncounteredTarget = table;
                } else {
                    Column column = table.getColumnByName(targetName.substring(separatorIndex + 1));
                    if (column != null) {
                        if (lastEncounteredTarget != null) throw new NameAmbigousException(targetName);
                        lastEncounteredTarget = column;
                    }
                }
            }
        } while (separatorIndex < targetName.length());
        return lastEncounteredTarget;
    }

    /**
     * Searches for a {@link Column} inside all of its known {@link Table}s.
     *
     * @param id
     * @return
     */
    Column findColumn(int id);

    @Override
    default Target.Type getType() {
        return Target.Type.SCHEMA;
    }
}
