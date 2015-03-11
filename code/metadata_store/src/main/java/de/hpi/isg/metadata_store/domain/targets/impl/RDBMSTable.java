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
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * The default implementation of the {@link Table}.
 *
 */
public class RDBMSTable extends AbstractRDBMSTarget implements Table {

    private static final long serialVersionUID = 8470123808962099640L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSTable.class);

    private static final boolean USE_STICKY_CACHE = true;

    @ExcludeHashCodeEquals
    private final Schema schema;

    @ExcludeHashCodeEquals
    private Reference<Collection<Column>> childColumnCache;

    /**
     * Experimental: keep the garbage collector from deleting the child column cache by keeping a firm reference to it.
     */
    @ExcludeHashCodeEquals
    private Collection<Column> stickyChildColumnCache;

    public static RDBMSTable buildAndRegisterAndAdd(final RDBMSMetadataStore observer, final Schema schema,
            final int id,
            final String name, String description, final Location location) {

        final RDBMSTable newTable = new RDBMSTable(observer, schema, id, name, description, location, true);
        newTable.register();
        // TODO: Remove
        // newTable.getSqlInterface().addTableToSchema(newTable, schema);
        return newTable;
    }

    public static RDBMSTable restore(final RDBMSMetadataStore observer, final Schema schema, final int id,
            final String name, String description, final Location location) {

        final RDBMSTable newTable = new RDBMSTable(observer, schema, id, name, description, location, false);
        return newTable;
    }

    private RDBMSTable(final RDBMSMetadataStore observer, final Schema schema, final int id, final String name,
            String description, final Location location, boolean isFreshlyCreated) {
        super(observer, id, name, description, location, isFreshlyCreated);
        this.schema = schema;
        if (isFreshlyCreated) {
            cacheChildColumns(new ArrayList<Column>());
        }
    }

    @Override
    public Column addColumn(final MetadataStore metadataStore, final String name, final String description,
            final int index) {
        Validate.isTrue(metadataStore instanceof RDBMSMetadataStore);
        Validate.isTrue(metadataStore.getSchemas().contains(getSchema()));
        IdUtils idUtils = metadataStore.getIdUtils();
        final int localSchemaId = idUtils.getLocalSchemaId(getId());
        final int localTableId = idUtils.getLocalTableId(getId());
        final int columnId = idUtils.createGlobalId(localSchemaId, localTableId, idUtils.getMinColumnNumber() + index);
        final Location location = new DefaultLocation();
        location.getProperties().put(Location.INDEX, index + "");
        final Column column = RDBMSColumn.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, columnId,
                name, description,
                location);
        addToChildIdCache(columnId);
        Collection<Column> columnCache = getChildColumnCache();
        if (columnCache != null) {
            columnCache.add(column);
        }
        return column;
    }

    @Override
    public Collection<Column> getColumns() {
        Collection<Column> columns = getChildColumnCache();
        if (columns == null) {
            LOGGER.trace("Column cache miss");
            columns = this.getSqlInterface().getAllColumnsForTable(this);
            cacheChildColumns(new ArrayList<>(columns));
        } else {
            LOGGER.trace("Column cache hit");
        }
        return Collections.unmodifiableCollection(columns);

    }

    @Override
    protected void store() {
        this.sqlInterface.addTableToSchema(this, this.schema);
    }

    /**
     * @return the parent schema
     */
    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public String toString() {
        return String.format("Table[%s, %08x]", getName(), getId());
    }

    @Override
    public Column getColumnByName(String name) throws NameAmbigousException {
        return this.getSqlInterface().getColumnByName(name, this);
    }

    @Override
    public Collection<Column> getColumnsByName(String name) {
        return this.getSqlInterface().getColumnsByName(name);
    }

    @Override
    public Column getColumnById(int columnId) {
        return this.getSqlInterface().getColumnById(columnId);
    }

    public void cacheChildColumns(Collection<Column> columns) {
        this.childColumnCache = new SoftReference<Collection<Column>>(columns);
        if (USE_STICKY_CACHE) {
            this.stickyChildColumnCache = columns;
        }
    }

    private Collection<Column> getChildColumnCache() {
        if (USE_STICKY_CACHE) {
            return this.stickyChildColumnCache;
        }
        if (this.childColumnCache == null) {
            return null;
        }
        Collection<Column> childColumns = this.childColumnCache.get();
        if (childColumns == null) {
            this.childColumnCache = null;
        }
        return childColumns;
    }
}
