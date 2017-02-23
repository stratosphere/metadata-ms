package de.hpi.isg.mdms.domain.targets;

import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.exceptions.MetadataStoreException;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * The default implementation of the {@link Table}.
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
        return newTable;
    }

    public static RDBMSTable restore(final RDBMSMetadataStore observer, final Schema schema, final int id,
                                     final String name, String description, final Location location) {
        return new RDBMSTable(observer, schema, id, name, description, location, false);
    }

    private RDBMSTable(final RDBMSMetadataStore observer, final Schema schema, final int id, final String name,
                       String description, final Location location, boolean isFreshlyCreated) {
        super(observer, id, name, description, location, isFreshlyCreated);
        this.schema = schema;
        if (isFreshlyCreated) {
            this.cacheChildColumns(new ArrayList<>());
        }
    }

    @Override
    public Column addColumn(final MetadataStore metadataStore, final String name, final String description,
                            final int index) {
        Validate.isTrue(metadataStore instanceof RDBMSMetadataStore);
        Validate.isTrue(metadataStore.getSchemas().contains(this.getSchema()));
        IdUtils idUtils = metadataStore.getIdUtils();
        final int localSchemaId = idUtils.getLocalSchemaId(this.getId());
        final int localTableId = idUtils.getLocalTableId(this.getId());
        final int columnId = idUtils.createGlobalId(localSchemaId, localTableId, idUtils.getMinColumnNumber() + index);
        final Location location = new DefaultLocation();
        location.getProperties().put(Location.INDEX, index + "");
        final Column column = RDBMSColumn.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, columnId,
                name, description, location
        );
        this.addToChildIdCache(columnId);
        Collection<Column> columnCache = this.getChildColumnCache();
        if (columnCache != null) {
            columnCache.add(column);
        }
        return column;
    }

    @Override
    public Collection<Column> getColumns() {
        Collection<Column> columns = this.getChildColumnCache();
        if (columns == null) {
            LOGGER.trace("Column cache miss");
            try {
                columns = this.getSqlInterface().getAllColumnsForTable(this);
            } catch (SQLException e) {
                throw new MetadataStoreException(e);
            }
            this.cacheChildColumns(new ArrayList<>(columns));
        } else {
            LOGGER.trace("Column cache hit");
        }
        return Collections.unmodifiableCollection(columns);

    }

    @Override
    public void store() {
        try {
            this.getSqlInterface().addTableToSchema(this, this.schema);
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
        return String.format("Table[%s, %08x]", this.getName(), this.getId());
    }

    @Override
    public Column getColumnByName(String name) throws NameAmbigousException {
        Collection<Column> columns = this.getColumnsByName(name);
        if (columns.isEmpty()) return null;
        if (columns.size() == 1) return columns.iterator().next();
        throw new NameAmbigousException(String.format(
                "Multiple columns named \"%s\" in \"%s\".", name, this
        ));
    }

    @Override
    public Collection<Column> getColumnsByName(String name) {
        return this.getColumns().stream()
                .filter(column -> column.getName().equals(name))
                .collect(Collectors.toList());
    }

    @Override
    public Column getColumnById(int columnId) {
        try {
            return this.getSqlInterface().getColumnById(columnId);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    private void cacheChildColumns(Collection<Column> columns) {
        this.childColumnCache = new SoftReference<>(columns);
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
