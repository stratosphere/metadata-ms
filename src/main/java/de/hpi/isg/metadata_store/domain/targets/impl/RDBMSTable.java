package de.hpi.isg.metadata_store.domain.targets.impl;

import java.util.Collection;
import java.util.Collections;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.impl.AbstractRDBMSTarget;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.util.IdUtils;

/**
 * The default implementation of the {@link Table}.
 *
 */
public class RDBMSTable extends AbstractRDBMSTarget implements Table {

    private static final long serialVersionUID = 8470123808962099640L;

    @ExcludeHashCodeEquals
    private final Schema schema;

    public static Table buildAndRegisterAndAdd(final RDBMSMetadataStore observer, final Schema schema, final int id,
            final String name,
            final Location location) {
        final RDBMSTable newTable = new RDBMSTable(observer, schema, id, name, location);
        newTable.register();
        newTable.getSqlInterface().addTableToSchema(newTable, schema);
        return newTable;
    }

    public static Table buildAndRegisterAndAdd(final RDBMSMetadataStore observer, final Schema schema,
            final String name,
            final Location location) {
        final RDBMSTable newTable = new RDBMSTable(observer, schema, -1, name, location);
        newTable.register();
        newTable.getSqlInterface().addTableToSchema(newTable, schema);
        return newTable;
    }

    public static Table build(final RDBMSMetadataStore observer, final Schema schema, final int id,
            final String name,
            final Location location) {
        final RDBMSTable newTable = new RDBMSTable(observer, schema, id, name, location);
        return newTable;
    }

    private RDBMSTable(final RDBMSMetadataStore observer, final Schema schema, final int id, final String name,
            final Location location) {
        super(observer, id, name, location);
        this.schema = schema;
    }

    @Override
    public Table addColumn(final Column column) {
        throw new RuntimeException(new OperationNotSupportedException());
    }

    @Override
    public Column addColumn(final MetadataStore metadataStore, final String name, final int index) {
        Validate.isTrue(metadataStore instanceof RDBMSMetadataStore);
        Validate.isTrue(metadataStore.getSchemas().contains(getSchema()));
        final int localSchemaId = IdUtils.getLocalSchemaId(getId());
        final int localTableId = IdUtils.getLocalTableId(getId());
        final int columnId = IdUtils.createGlobalId(localSchemaId, localTableId, IdUtils.MIN_COLUMN_NUMBER + index);
        final IndexedLocation location = new IndexedLocation(index, getLocation());
        final Column column = RDBMSColumn.buildAndRegisterAndAdd((RDBMSMetadataStore) metadataStore, this, columnId,
                name,
                location);
        return column;
    }

    @Override
    public Collection<Column> getColumns() {
        return Collections.unmodifiableCollection(this.getSqlInterface().getAllColumnsForTable(this));
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
        return String.format("Table[%s, %d columns, %08x]", getName(), getColumns().size(), getId());
    }
}
