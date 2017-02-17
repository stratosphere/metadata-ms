package de.hpi.isg.mdms.tools.metanome;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.util.NoSuchElementException;

/**
 * This utility helps to resolve Metanome identifiers to {@link Table}s and {@link Column}s.
 */
public class IdentifierResolver {


    /**
     * Metanome does not have the notion of schemata, so we require to have a given {@link Schema} which the
     * imported metadata describe.
     */
    private final Schema schema;

    private final MetadataStore metadataStore;

    /**
     * Creates a new instance.
     *
     * @param metadataStore within which the {@code schema} resides
     * @param schema        within which the Metanome identifiers should be resolved
     */
    public IdentifierResolver(MetadataStore metadataStore, Schema schema) {
        this.schema = schema;
        this.metadataStore = metadataStore;
    }

    /**
     * Resolve a {@link ColumnIdentifier} to a {@link Column}.
     *
     * @throws IllegalArgumentException if the {@link Column} could not be found
     */
    public Column resolveColumn(ColumnIdentifier columnIdentifier) throws IllegalArgumentException {
        Table table = this.resolveTable(columnIdentifier.getTableIdentifier());
        return this.resolveColumn(table, columnIdentifier.getColumnIdentifier());
    }

    private Column resolveColumn(Table table, String columnIdentifier) {
        try {
            return table.getColumnByName(columnIdentifier);
        } catch (NoSuchElementException e) {
                // Pass.
        }

        // Detect Metanome's fallback column names.
        if (columnIdentifier.startsWith("column")) {
            try {
                int columnPosition = Integer.parseInt(columnIdentifier.substring(6));
                IdUtils idUtils = this.metadataStore.getIdUtils();
                int localSchemaId = idUtils.getLocalSchemaId(table.getId());
                int localTableId = idUtils.getLocalTableId(table.getId());
                int columnId = idUtils.createGlobalId(localSchemaId, localTableId, columnPosition - 1);
                Column column = table.getColumnById(columnId);
                if (column != null) return column;
            } catch (NumberFormatException | NoSuchElementException e) {
                // Pass.
            }
        }

        throw new IllegalArgumentException(String.format("Cannot find a column named \"%s\" in %s.", columnIdentifier, table));
    }

    /**
     * Resolves a {@link Table} by its name.
     *
     * @param tableIdentifier the name of the {@link Table}
     * @return the {@link Table}
     * @throws IllegalArgumentException if the {@link Table} could not be found
     */
    public Table resolveTable(String tableIdentifier) throws IllegalArgumentException {
        // Try to resolve the table name immediately.
        Table table = this.schema.getTableByName(tableIdentifier);
        if (table != null) return table;

        // Try to strip of any file extension comprised in the table name.
        int extensionIndex = tableIdentifier.lastIndexOf('.');
        if (extensionIndex != -1) {
            String trimmedTableIdentifier = tableIdentifier.substring(0, extensionIndex);
            table = this.schema.getTableByName(trimmedTableIdentifier);
            if (table != null) return table;
        }

        throw new IllegalArgumentException(String.format("Cannot find a table named \"%s\".", tableIdentifier));
    }

}
