package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.impl.RDBMSTarget;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

public class RDBMSSchema extends RDBMSTarget implements Schema {

    private static final long serialVersionUID = -6940399614326634190L;

    private RDBMSSchema(RDBMSMetadataStore observer, int id, String name, Location location) {
        super(observer, id, name, location);
    }

    public static Schema buildAndRegisterAndAdd(RDBMSMetadataStore observer, int id, String name,
            Location location) {
        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, location);
        newSchema.register();
        newSchema.getSqlInterface().addSchema(newSchema);
        return newSchema;
    }

    public static Schema buildAndRegisterAndAdd(RDBMSMetadataStore observer, String name,
            Location location) {
        final RDBMSSchema newSchema = new RDBMSSchema(observer, -1, name, location);
        newSchema.register();
        newSchema.getSqlInterface().addSchema(newSchema);
        return newSchema;
    }

    public static Schema build(RDBMSMetadataStore observer, int id, String name,
            Location location) {
        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, location);
        return newSchema;
    }

    @Override
    public Table addTable(final MetadataStore metadataStore, final String name, final Location location) {
        Validate.isTrue(metadataStore instanceof RDBMSMetadataStore);
        Validate.isTrue(metadataStore.getSchemas().contains(this));
        final int tableId = metadataStore.getUnusedTableId(this);
        final Table table = RDBMSTable.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, tableId, name,
                location);
        return table;
    }

    @Override
    public Schema addTable(final Table table) {
        throw new RuntimeException(new OperationNotSupportedException());
    }

    @Override
    public Table getTable(final String name) throws NameAmbigousException {
        final List<Table> results = new ArrayList<>();
        for (final Table table : this.getSqlInterface().getAllTablesForSchema(this)) {
            if (table.getName().equals(name)) {
                results.add(table);
            }
        }
        if (results.size() > 1) {
            throw new NameAmbigousException(name);
        }
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    @Override
    public Collection<Table> getTables() {
        return this.getSqlInterface().getAllTablesForSchema(this);
    }

    @Override
    public Column findColumn(final int columnId) {
        for (final Table table : getTables()) {
            for (final Column column : table.getColumns()) {
                if (column.getId() == columnId) {
                    return column;
                }
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return String.format("Schema[%s, %d tables, %08x]", this.getName(), this.getTables().size(), this.getId());
    }

}
