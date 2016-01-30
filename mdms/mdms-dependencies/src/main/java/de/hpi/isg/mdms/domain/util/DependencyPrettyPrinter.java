package de.hpi.isg.mdms.domain.util;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.BasicPrettyPrinter;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.util.CollectionUtils;

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
        int depSchemaId = idUtils.getSchemaId(ind.getTargetReference().getDependentColumns()[0]);
        Schema depSchema = this.metadataStore.getSchemaById(depSchemaId);
        int depTableId = idUtils.getTableId(ind.getTargetReference().getDependentColumns()[0]);
        Table depTable = depSchema.getTableById(depTableId);
        sb.append(depTable.getName()).append("[");
        String separator = "";
        for (int columnId : ind.getTargetReference().getDependentColumns()) {
            sb.append(separator).append(depTable.getColumnById(columnId).getName());
            separator = ", ";
        }
        sb.append("] < ");
        int refSchemaId = idUtils.getSchemaId(ind.getTargetReference().getReferencedColumns()[0]);
        Schema refSchema = this.metadataStore.getSchemaById(refSchemaId);
        int refTableId = idUtils.getTableId(ind.getTargetReference().getReferencedColumns()[0]);
        Table refTable = refSchema.getTableById(refTableId);
        sb.append(refTable.getName()).append("[");
        separator = "";
        for (int columnId : ind.getTargetReference().getReferencedColumns()) {
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
        final int anyColumnId = CollectionUtils.getAny(ucc.getTargetReference().getAllTargetIds());
        int schemaId = idUtils.getSchemaId(anyColumnId);
        Schema schema = this.metadataStore.getSchemaById(schemaId);
        int tableId = idUtils.getTableId(anyColumnId);
        Table table = schema.getTableById(tableId);
        sb.append(table.getName()).append("[");
        String separator = "";
        for (int columnId : ucc.getTargetReference().getAllTargetIds()) {
            sb.append(separator).append(tryToGetTableName(table, columnId));
            separator = ", ";
        }
        sb.append("]");
        return sb.toString();
    }

    public IdUtils getIdUtils() {
        return this.metadataStore.getIdUtils();
    }
}
