package de.hpi.isg.mdms.domain.util;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.BasicPrettyPrinter;
import de.hpi.isg.mdms.model.util.IdUtils;

/**
 * Utility to pretty-print IDs, dependencies etc.
 */
public class DependencyPrettyPrinter extends BasicPrettyPrinter {

    public DependencyPrettyPrinter(MetadataStore metadataStore) {
        super(metadataStore);
    }

    /**
     * Pretty-prints the given IND.
     *
     * @param ind an IND
     * @return the pretty-printed IND
     */
    public String prettyPrint(InclusionDependency ind) {
        StringBuffer sb = new StringBuffer();
        IdUtils idUtils = this.metadataStore.getIdUtils();
        int depSchemaId = idUtils.getSchemaId(ind.getDependentColumnIds()[0]);
        Schema depSchema = this.metadataStore.getSchemaById(depSchemaId);
        int depTableId = idUtils.getTableId(ind.getDependentColumnIds()[0]);
        Table depTable = depSchema.getTableById(depTableId);
        sb.append(depTable.getName()).append("[");
        String separator = "";
        for (int columnId : ind.getDependentColumnIds()) {
            sb.append(separator).append(depTable.getColumnById(columnId).getName());
            separator = ", ";
        }
        sb.append("] < ");
        int refSchemaId = idUtils.getSchemaId(ind.getReferencedColumnIds()[0]);
        Schema refSchema = this.metadataStore.getSchemaById(refSchemaId);
        int refTableId = idUtils.getTableId(ind.getReferencedColumnIds()[0]);
        Table refTable = refSchema.getTableById(refTableId);
        sb.append(refTable.getName()).append("[");
        separator = "";
        for (int columnId : ind.getReferencedColumnIds()) {
            sb.append(separator).append(refTable.getColumnById(columnId).getName());
            separator = ", ";
        }
        sb.append("]");
        return sb.toString();
    }


    /**
     * Pretty-prints the given UCC.
     *
     * @param ucc a UCC
     * @return the pretty-printed UCC
     */
    public String prettyPrint(UniqueColumnCombination ucc) {
        StringBuffer sb = new StringBuffer();
        IdUtils idUtils = this.metadataStore.getIdUtils();
        final int anyColumnId = ucc.getColumnIds()[0];
        int schemaId = idUtils.getSchemaId(anyColumnId);
        Schema schema = this.metadataStore.getSchemaById(schemaId);
        int tableId = idUtils.getTableId(anyColumnId);
        Table table = schema.getTableById(tableId);
        sb.append(table.getName()).append("[");
        String separator = "";
        for (int columnId : ucc.getColumnIds()) {
            sb.append(separator).append(this.tryToGetTableName(table, columnId));
            separator = ", ";
        }
        sb.append("]");
        return sb.toString();
    }

    public IdUtils getIdUtils() {
        return this.metadataStore.getIdUtils();
    }
}
