package de.hpi.isg.metadata_store.domain.impl;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.RDBMSTable;
import de.hpi.isg.metadata_store.domain.util.IdUtils;
import de.hpi.isg.metadata_store.domain.util.LocationCache;
import de.hpi.isg.metadata_store.exceptions.NameAmbigousException;

/**
 * The default implementation of the {@link MetadataStore} for storing the data inside of a RDBMS.
 * 
 */

public class RDBMSMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final String NUM_COLUMN_BITS_IN_IDS_KEY = "numColumnBitsInIds";

    private static final String NUM_TABLE_BITS_IN_IDS_KEY = "numTableBitsInIds";

    private static final long serialVersionUID = 400271996998552017L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSMetadataStore.class);

    @ExcludeHashCodeEquals
    transient final SQLInterface sqlInterface;

    @ExcludeHashCodeEquals
    transient final Random randomGenerator = new Random();

    @ExcludeHashCodeEquals
    transient final IdUtils idUtils;

    @ExcludeHashCodeEquals
    transient final LocationCache locationCache = new LocationCache();

    public static RDBMSMetadataStore createNewInstance(SQLiteInterface sqliteInterface) {
        return createNewInstance(sqliteInterface, IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    public static RDBMSMetadataStore createNewInstance(SQLiteInterface sqlInterface, int numTableBitsInIds,
            int numColumnBitsInIds) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(NUM_TABLE_BITS_IN_IDS_KEY, String.valueOf(numTableBitsInIds));
        configuration.put(NUM_COLUMN_BITS_IN_IDS_KEY, String.valueOf(numColumnBitsInIds));
        return RDBMSMetadataStore.createNewInstance(sqlInterface, configuration);
    }

    public static RDBMSMetadataStore createNewInstance(SQLiteInterface sqliteInterface,
            Map<String, String> configuration) {
        if (sqliteInterface.allTablesExist()) {
            LOGGER.warn("The metadata store will be overwritten.");
        }
        sqliteInterface.initializeMetadataStore();
        RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqliteInterface, configuration);
        sqliteInterface.saveConfiguration();
        return metadataStore;
    }

    public static RDBMSMetadataStore load(SQLiteInterface sqliteInterface) {
        if (!sqliteInterface.allTablesExist()) {
            throw new IllegalStateException("The metadata store does not seem to be initialized.");
        }
        Map<String, String> configuration = sqliteInterface.loadConfiguration();
        RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqliteInterface, configuration);
        metadataStore.fillLocationCache();
        return metadataStore;
    }

    private RDBMSMetadataStore(SQLiteInterface sqliteInterface, Map<String, String> configuration) {
        this.sqlInterface = sqliteInterface;
        this.sqlInterface.setMetadataStore(this);
        this.setUseJournal(true);
        int numTableBitsInIds = Integer.valueOf(configuration.get(NUM_TABLE_BITS_IN_IDS_KEY));
        int numColumnBitsInIds = Integer.valueOf(configuration.get(NUM_COLUMN_BITS_IN_IDS_KEY));
        this.idUtils = new IdUtils(numTableBitsInIds, numColumnBitsInIds);
    }

    @SuppressWarnings("unchecked")
    private void fillLocationCache() {
        try {
            for (String locationClassName : this.sqlInterface.getLocationClassNames()) {
                this.locationCache.cacheLocationType((Class<? extends Location>) Class.forName(locationClassName));
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Could not fill location cache.", e);
        }
    }

    @Override
    public Schema addSchema(final String name, final String description, final Location location) {
        final int id = this.getUnusedSchemaId();
        final Schema schema = RDBMSSchema.buildAndRegisterAndAdd(this, id, name, description, location);
        return schema;
    }

    @Override
    public int generateRandomId() {
        final int id = Math.abs(this.randomGenerator.nextInt(Integer.MAX_VALUE));
        if (this.idIsInUse(id)) {
            return this.generateRandomId();
        }
        return id;
    }

    @Override
    public Schema getSchemaByName(final String schemaName) throws NameAmbigousException {
        return this.sqlInterface.getSchemaByName(schemaName);
    }

    @Override
    public Collection<Schema> getSchemas() {
        return Collections.unmodifiableCollection(this.sqlInterface.getAllSchemas());
    }

    @Override
    public int getUnusedSchemaId() {
        final int searchOffset = this.getSchemas().size();
        for (int baseSchemaNumber = this.idUtils.getMinSchemaNumber(); baseSchemaNumber <= this.idUtils
                .getMaxSchemaNumber(); baseSchemaNumber++) {
            int schemaNumber = baseSchemaNumber + searchOffset;
            schemaNumber = schemaNumber > this.idUtils.getMaxSchemaNumber() ? schemaNumber
                    - (this.idUtils.getMaxSchemaNumber() - this.idUtils.getMinSchemaNumber()) : schemaNumber;
            final int id = this.idUtils.createGlobalId(schemaNumber);
            if (!this.idIsInUse(id)) {
                return id;
            }
        }
        throw new IllegalStateException(String.format("No free schema ID left within schema."));
    }

    @Override
    public int getUnusedTableId(final Schema schema) {
        Validate.isTrue(this.sqlInterface.getAllSchemas().contains(schema));
        final int schemaNumber = this.idUtils.getLocalSchemaId(schema.getId());
        final int searchOffset = ((RDBMSSchema) schema).getNumTables() != -1 ?
                ((RDBMSSchema) schema).getNumTables() + 1 : schema.getTables().size();
        for (int baseTableNumber = this.idUtils.getMinTableNumber(); baseTableNumber <= this.idUtils
                .getMaxTableNumber(); baseTableNumber++) {
            int tableNumber = baseTableNumber + searchOffset;
            tableNumber = tableNumber > this.idUtils.getMaxTableNumber() ? tableNumber
                    - (this.idUtils.getMaxTableNumber() - this.idUtils.getMinTableNumber()) : tableNumber;
            final int id = this.idUtils.createGlobalId(schemaNumber, tableNumber);
            if (!this.idIsInUse(id)) {
                return id;
            }
        }
        throw new IllegalStateException(String.format("No free table ID left within schema %s.", schema));
    }

    @Override
    public boolean hasTargetWithId(int id) {
        return idIsInUse(id);
    }

    private boolean idIsInUse(final int id) {
        try {
            return this.sqlInterface.isTargetIdInUse(id);
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Could not determine if ID %s is in use.", id), e);
        }
        // synchronized (this.sqlInterface.getIdsInUse()) {
        //
        // return this.sqlInterface.getIdsInUse().contains(id);
        // }
    }

    // @Override
    // public void registerId(final int id) {
    // // synchronized (this.sqlInterface.getIdsInUse()) {
    // if (!this.sqlInterface.addToIdsInUse(id)) {
    // throw new IdAlreadyInUseException("id is already in use: " + id);
    // }
    // // }
    // }
    //
    @Override
    public void registerTargetObject(final Target target) {
        // Register the Location type of the target.
        if (this.locationCache.cacheLocationType(target.getLocation().getClass())) {
            try {
                this.sqlInterface.storeLocationType(target.getLocation().getClass());
            } catch (SQLException e) {
                throw new RuntimeException("Could not register location type.", e);
            }
        }

        ((AbstractRDBMSTarget) target).store();
        // this.sqlInterface.addTarget(target);
    }

    @Override
    public String toString() {
        return "MetadataStore[" + this.sqlInterface + "]";
    }

    @Override
    public Collection<ConstraintCollection> getConstraintCollections() {
        return this.sqlInterface.getAllConstraintCollections();
    }

    @Override
    public ConstraintCollection getConstraintCollection(int id) {
        return this.sqlInterface.getConstraintCollectionById(id);
    }

    public SQLInterface getSQLInterface() {
        return this.sqlInterface;
    }

    @Override
    public int getUnusedConstraintCollectonId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    @Override
    public ConstraintCollection createConstraintCollection(String description, Target... scope) {
        // Make sure that the given targets are actually compatible with this kind of metadata store.
        for (Target target : scope) {
            Validate.isAssignableFrom(AbstractRDBMSTarget.class, target.getClass());
        }
        ConstraintCollection constraintCollection = new RDBMSConstraintCollection(getUnusedConstraintCollectonId(),
                description,
                new HashSet<Target>(Arrays.asList(scope)), getSQLInterface());

        // Store the constraint collection in the DB.
        this.sqlInterface.addConstraintCollection(constraintCollection);
        for (Target target : constraintCollection.getScope()) {
            this.sqlInterface.addScope(target, constraintCollection);
        }

        return constraintCollection;
    }

    /**
     * @return the idUtils
     */
    @Override
    public IdUtils getIdUtils() {
        return idUtils;
    }

    /**
     * @return the locationCache
     */
    public LocationCache getLocationCache() {
        return locationCache;
    }

    /**
     * @return key value pairs that describe the configuration of this metadata store.
     */
    public Map<String, String> getConfiguration() {
        Map<String, String> configuration = new HashMap<String, String>();
        configuration.put(NUM_TABLE_BITS_IN_IDS_KEY, String.valueOf(this.idUtils.getNumTableBits()));
        configuration.put(NUM_COLUMN_BITS_IN_IDS_KEY, String.valueOf(this.idUtils.getNumColumnBits()));
        return configuration;
    }

    @Override
    public void save(String path) {
        LOGGER.warn("save(path) does not take into account the save path.");
        try {
            flush();
        } catch (Exception e) {
            LOGGER.error("Flushing the metadata store failed.", e);
        }
    }

    @Override
    public void flush() throws Exception {
        this.sqlInterface.saveConfiguration();
        this.sqlInterface.flush();
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) {
        return this.sqlInterface.getSchemasByName(schemaName);
    }

    @Override
    public Schema getSchemaById(int schemaId) {
        return this.sqlInterface.getSchemaById(schemaId);
    }

    @Override
    public void removeSchema(Schema schema) {
        try {
            this.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (Table table : schema.getTables()) {
            for (Column column : table.getColumns()) {
                checkIfInScopeAndDelete(column);
                sqlInterface.removeColumn((RDBMSColumn) column);
            }
            checkIfInScopeAndDelete(table);
            sqlInterface.removeTable((RDBMSTable) table);
        }
        checkIfInScopeAndDelete(schema);
        sqlInterface.removeSchema((RDBMSSchema) schema);
        try {
            this.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkIfInScopeAndDelete(Target target) {
        for (ConstraintCollection collection : this.getConstraintCollections()) {
            if (collection.getScope().contains(target)) {
                this.removeConstraintCollection(collection);
            }
        }
    }

    @Override
    public void removeConstraintCollection(ConstraintCollection constraintCollection) {
        sqlInterface.removeConstraintCollection(constraintCollection);
    }

    public void setUseJournal(boolean isUseJournal) {
        this.sqlInterface.setUseJournal(isUseJournal);
    }
}
