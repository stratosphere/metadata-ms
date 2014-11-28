package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.impl.AbstractRDBMSTarget;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

public class RDBMSSchema extends AbstractRDBMSTarget implements Schema {

    private static final long serialVersionUID = -6940399614326634190L;

    private RDBMSSchema(RDBMSMetadataStore observer, int id, String name, Location location, boolean isFreshlyCreated) {
        super(observer, id, name, location, isFreshlyCreated);
    }

    public static RDBMSSchema buildAndRegisterAndAdd(RDBMSMetadataStore observer, int id, String name,
            Location location) {
        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, location, true);
        newSchema.register();
        newSchema.getSqlInterface().addSchema(newSchema);
        return newSchema;
    }

    public static RDBMSSchema buildAndRegisterAndAdd(RDBMSMetadataStore observer, String name,
            Location location) {

        return buildAndRegisterAndAdd(observer, -1, name, location);
    }

    public static RDBMSSchema restore(RDBMSMetadataStore observer, int id, String name,
            Location location) {

        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, location, false);
        return newSchema;
    }

    @Override
    public Table addTable(final MetadataStore metadataStore, final String name, final Location location) {
        Validate.isTrue(metadataStore instanceof RDBMSMetadataStore);
        Validate.isTrue(metadataStore.getSchemas().contains(this));
        final int tableId = metadataStore.getUnusedTableId(this);
        final Table table = RDBMSTable.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, tableId, name,
                location);
        addToChildIdCache(tableId);
        return table;
    }

    @Override
    public Schema addTable(final Table table) {
        throw new RuntimeException(new OperationNotSupportedException());
    }

    @Override
    public Table getTableByName(final String name) throws NameAmbigousException {
        return this.getSqlInterface().getTableByName(name);
    }

    @Override
    public Collection<Table> getTablesByName(String name) {
        return this.getSqlInterface().getTablesByName(name);
    }

    @Override
    public Table getTableById(int tableId) {
        return this.getSqlInterface().getTableById(tableId);
    }

    @Override
    public Collection<Table> getTables() {
        return Collections.unmodifiableCollection(this.getSqlInterface().getAllTablesForSchema(this));
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
