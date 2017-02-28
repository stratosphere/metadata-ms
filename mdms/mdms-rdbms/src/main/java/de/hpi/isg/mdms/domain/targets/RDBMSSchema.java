package de.hpi.isg.mdms.domain.targets;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.exceptions.MetadataStoreException;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.util.LRUCache;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class RDBMSSchema extends AbstractRDBMSTarget implements Schema {

    /**
     * Stores the number of tables in this schema to quickly find free IDs for new tables.
     */
    @ExcludeHashCodeEquals
    private transient int numTables = -1;

    @ExcludeHashCodeEquals
    private final transient LRUCache<Integer, RDBMSTable> childTableCache = new LRUCache<>(1000);

    @ExcludeHashCodeEquals
    private transient boolean isChildTableCacheComplete;

    private RDBMSSchema(RDBMSMetadataStore observer, int id, String name, String description, Location location,
                        boolean isFreshlyCreated) {
        super(observer, id, name, description, location, isFreshlyCreated);
        if (isFreshlyCreated) {
            this.isChildTableCacheComplete = true;
        }
    }

    public static RDBMSSchema buildAndRegisterAndAdd(RDBMSMetadataStore observer, int id, String name,
                                                     String description,
                                                     Location location) {
        final RDBMSSchema newSchema = new RDBMSSchema(observer, id, name, description, location, true);
        newSchema.register();
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
        final RDBMSTable table = RDBMSTable.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, tableId, name,
                description,
                location);
        this.childTableCache.put(tableId, table);
        if (this.numTables != -1) {
            this.numTables++;
        }
        return table;
    }

    @Override
    public void store() throws SQLException {
        this.getSqlInterface().addSchema(this);
    }

    @Override
    public Table getTableByName(final String name) throws NameAmbigousException {
        Collection<Table> tables = this.getTablesByName(name);
        if (tables.isEmpty()) return null;
        if (tables.size() == 1) return tables.iterator().next();
        throw new NameAmbigousException(
                String.format("Multiple tables are named \"%s\" in \"%s\".", name, this.getName())
        );
    }

    @Override
    public Collection<Table> getTablesByName(String name) {
        // NB: We must not answer the query from the cache, as the cache is not guaranteeing to serve all tables
        // with a certain name.
        try {
            Collection<Table> tables = this.getSqlInterface().getTablesByName(name, this);
            Collection<Table> matchingTables = new ArrayList<>(1);
            // Put the tables into the cache and look for a match.
            for (Table table : tables) {
                if (table.getName().equals(name)) {
                    matchingTables.add(table);
                }
                this.childTableCache.put(table.getId(), (RDBMSTable) table);
            }
            return matchingTables;
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Table getTableById(int tableId) {
        // Try to serve the table from the cache.
        RDBMSTable table = this.childTableCache.get(tableId);
        if (table != null || this.isChildTableCacheComplete) return table;

        // Otherwise, load all tables and repeat.
        try {
            table = (RDBMSTable) this.getSqlInterface().getTableById(tableId);
            this.childTableCache.put(table.getId(), table);
            return table;
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Collection<Table> getTables() {
        if (this.isChildTableCacheComplete) {
            return Collections.unmodifiableCollection(new ArrayList<>(this.childTableCache.values()));
        }

        this.childTableCache.setEvictionEnabled(false);
        try {
            for (Table table : this.getSqlInterface().getAllTablesForSchema(this)) {
                this.childTableCache.put(table.getId(), (RDBMSTable) table);
            }
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }

        this.numTables = this.childTableCache.size();
        return Collections.unmodifiableCollection(new ArrayList<>(this.childTableCache.values()));
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
        return this.numTables;
    }

    @Override
    public String toString() {
        return String.format("Schema[%s, %s, %08x]", this.getName(), this.getDescription(), this.getId());
    }
}
