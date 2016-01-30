package de.hpi.isg.mdms.model.util;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;

/**
 * Utility to pretty-print schema elements by their ID.
 */
public class BasicPrettyPrinter {

    protected final MetadataStore metadataStore;

    public BasicPrettyPrinter(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }


    protected String tryToGetTableName(Table depTable, int columnId) {
        try {
            return depTable.getColumnById(columnId).getName();
        } catch (Throwable t) {
            return "???";
        }
    }

    /**
     * Pretty-prints schema elements.
     *
     * @param id the ID of a schema element
     * @return the pretty-printed schema element
     */
    public String prettyPrint(int id) {
        final IdUtils idUtils = metadataStore.getIdUtils();
        if (idUtils.isSchemaId(id)) {
            int schemaId = id;
            final Schema schema = metadataStore.getSchemaById(schemaId);
            return String.format("%s", schema.getName());

        } else if (idUtils.isTableId(id)) {
            int schemaId = idUtils.getSchemaId(id);
            int tableId = id;
            final Schema schema = metadataStore.getSchemaById(schemaId);
            final Table table = schema.getTableById(tableId);
            return String.format("%s.%s", schema.getName(), table.getName());
        } else {
            int schemaId = idUtils.getSchemaId(id);
            int tableId = idUtils.getTableId(id);
            int columnId = id;
            final Schema schema = metadataStore.getSchemaById(schemaId);
            final Table table = schema.getTableById(tableId);
            final Column column = table.getColumnById(columnId);
            return String.format("%s.%s.%s", schema.getName(), table.getName(), column.getName());
        }
    }


    public IdUtils getIdUtils() {
        return this.metadataStore.getIdUtils();
    }
}
