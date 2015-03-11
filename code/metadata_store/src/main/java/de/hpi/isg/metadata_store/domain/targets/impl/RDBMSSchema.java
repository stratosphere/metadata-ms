package de.hpi.isg.metadata_store.domain.targets.impl;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractRDBMSTarget;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

public class RDBMSSchema extends AbstractRDBMSTarget implements Schema {

    private static final long serialVersionUID = -6940399614326634190L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSSchema.class);

    /**
     * Stores the number of tables in this schema to quickly find free IDs for new tables.
     */
    @ExcludeHashCodeEquals
    private int numTables = -1;

    @ExcludeHashCodeEquals
    private Reference<Collection<Table>> childTableCache;

    private RDBMSSchema(RDBMSMetadataStore observer, int id, String name, String description, Location location,
            boolean isFreshlyCreated) {
        super(observer, id, name, description, location, isFreshlyCreated);
        if (isFreshlyCreated) {
            cacheChildTables(new ArrayList<Table>());
        }
    }

    public static RDBMSSchema buildAndRegisterAndAdd(RDBMSMetadataStore observer, int id, String name,
            String description,
            Location location) {
        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, description, location, true);
        newSchema.register();
        // TODO: remove
        // newSchema.getSqlInterface().addSchema(newSchema);
        newSchema.numTables = 0;
        return newSchema;
    }

    public static RDBMSSchema buildAndRegisterAndAdd(RDBMSMetadataStore observer, String name, String description,
            Location location) {

        return buildAndRegisterAndAdd(observer, -1, name, description, location);
    }

    public static RDBMSSchema restore(RDBMSMetadataStore observer, int id, String name,
            String description, Location location) {

        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, description, location, false);
        return newSchema;
    }

    @Override
    public Table addTable(final MetadataStore metadataStore, final String name, final String description,
            final Location location) {
        Validate.isTrue(metadataStore instanceof RDBMSMetadataStore);
        Collection<Schema> schemas = metadataStore.getSchemas();
        Validate.isTrue(schemas.contains(this));
        final int tableId = metadataStore.getUnusedTableId(this);
        final Table table = RDBMSTable.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, tableId, name,
                description,
                location);
        addToChildIdCache(tableId);
        if (this.numTables != -1) {
            this.numTables++;
        }
        Collection<Table> childTableCache = getChildTableCache();
        if (childTableCache != null) {
            childTableCache.add(table);
        }
        return table;
    }

    @Override
    protected void store() {
        this.sqlInterface.addSchema(this);
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
        Collection<Table> tables = getChildTableCache();
        if (tables == null) {
            LOGGER.trace("Table cache miss");
            tables = this.getSqlInterface().getAllTablesForSchema(this);
            cacheChildTables(new ArrayList<>(tables));
        } else {
            LOGGER.trace("Table cache hit");
        }
        return Collections.unmodifiableCollection(tables);
    }

    public void cacheChildTables(Collection<Table> tables) {
        this.childTableCache = new SoftReference<Collection<Table>>(tables);
    }

    private Collection<Table> getChildTableCache() {
        if (this.childTableCache == null) {
            return null;
        }
        Collection<Table> childTables = this.childTableCache.get();
        if (childTables == null) {
            this.childTableCache = null;
        }
        return childTables;
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

    /**
     * @return the number of tables in this schema or -1 if it is unknown.
     */
    public int getNumTables() {
        return numTables;
    }

    @Override
    public String toString() {
        return String.format("Schema[%s, %s, %08x]", this.getName(), this.getDescription(), this.getId());
    }
}
