package de.hpi.isg.mdms.domain;

import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.domain.experiment.RDBMSAlgorithm;
import de.hpi.isg.mdms.domain.experiment.RDBMSExperiment;
import de.hpi.isg.mdms.domain.targets.AbstractRDBMSTarget;
import de.hpi.isg.mdms.domain.targets.RDBMSColumn;
import de.hpi.isg.mdms.domain.targets.RDBMSSchema;
import de.hpi.isg.mdms.domain.targets.RDBMSTable;
import de.hpi.isg.mdms.exceptions.MetadataStoreException;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * The default implementation of the {@link MetadataStore} for storing the data inside of a RDBMS. The choice of
 * the underlying database is given through the use of an appropriate {@link de.hpi.isg.mdms.rdbms.SQLInterface}.
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
    transient final Int2ObjectMap<String> codeDictionary = new Int2ObjectOpenHashMap<>();
    @ExcludeHashCodeEquals
    transient final Object2IntMap<String> reverseCodeDictionary = new Object2IntOpenHashMap<>();

    @ExcludeHashCodeEquals
    transient Collection<ConstraintCollection<?>> constraintCollectionCache = null;

    public static RDBMSMetadataStore createNewInstance(SQLInterface sqlInterface) throws SQLException {
        return createNewInstance(sqlInterface, IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    public static RDBMSMetadataStore createNewInstance(SQLInterface sqlInterface, int numTableBitsInIds,
                                                       int numColumnBitsInIds) throws SQLException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(NUM_TABLE_BITS_IN_IDS_KEY, String.valueOf(numTableBitsInIds));
        configuration.put(NUM_COLUMN_BITS_IN_IDS_KEY, String.valueOf(numColumnBitsInIds));
        return RDBMSMetadataStore.createNewInstance(sqlInterface, configuration);
    }

    public static RDBMSMetadataStore createNewInstance(SQLInterface sqlInterface,
                                                       Map<String, String> configuration) throws SQLException {
        if (sqlInterface.checkAllTablesExistence()) {
            LOGGER.warn("The metadata store will be overwritten.");
        }
        sqlInterface.initializeMetadataStore();
        RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqlInterface, configuration);
        sqlInterface.saveConfiguration();
        return metadataStore;
    }

    public static RDBMSMetadataStore load(SQLInterface sqlInterface) throws SQLException {
        if (!sqlInterface.checkAllTablesExistence()) {
            throw new IllegalStateException("The metadata store does not seem to be initialized.");
        }
        Map<String, String> configuration = sqlInterface.loadConfiguration();
        RDBMSMetadataStore metadataStore = new RDBMSMetadataStore(sqlInterface, configuration);
        return metadataStore;
    }

    private RDBMSMetadataStore(SQLInterface sqlInterface, Map<String, String> configuration) {
        this.sqlInterface = sqlInterface;
        this.sqlInterface.setMetadataStore(this);
        this.setUseJournal(true);
        int numTableBitsInIds = Integer.valueOf(configuration.get(NUM_TABLE_BITS_IN_IDS_KEY));
        int numColumnBitsInIds = Integer.valueOf(configuration.get(NUM_COLUMN_BITS_IN_IDS_KEY));
        this.idUtils = new IdUtils(numTableBitsInIds, numColumnBitsInIds);
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
        if (this.isIdInUse(id)) {
            return this.generateRandomId();
        }
        return id;
    }

    @Override
    public Target getTargetById(int targetId) {
        try {
            switch (this.idUtils.getIdType(targetId)) {
                case COLUMN:
                    return this.sqlInterface.getColumnById(targetId);
                case TABLE:
                    return this.sqlInterface.getTableById(targetId);
                case SCHEMA:
                    return this.sqlInterface.getSchemaById(targetId);
                default:
                    throw new IllegalArgumentException("Invalid ID.");
            }
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Schema getSchemaByName(final String schemaName) throws NameAmbigousException {
        Collection<Schema> schemas = this.getSchemasByName(schemaName);
        if (schemas.isEmpty()) return null;
        if (schemas.size() == 1) return schemas.iterator().next();
        throw new NameAmbigousException(String.format("Found %d schemas named \"%s\".", schemas.size(), schemaName));
    }

    @Override
    public Collection<Schema> getSchemas() {
        try {
            return Collections.unmodifiableCollection(this.sqlInterface.getAllSchemas());
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
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
            if (!this.isIdInUse(id)) {
                return id;
            }
        }
        throw new IllegalStateException(String.format("No free schema ID left within schema."));
    }

    @Override
    public int getUnusedTableId(final Schema schema) {
        try {
            Validate.isTrue(this.sqlInterface.getAllSchemas().contains(schema));
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
        final int schemaNumber = this.idUtils.getLocalSchemaId(schema.getId());
        final int searchOffset = ((RDBMSSchema) schema).getNumTables() != -1 ?
                ((RDBMSSchema) schema).getNumTables() : schema.getTables().size();
        for (int baseTableNumber = this.idUtils.getMinTableNumber(); baseTableNumber <= this.idUtils
                .getMaxTableNumber(); baseTableNumber++) {
            int tableNumber = baseTableNumber + searchOffset;
            tableNumber = tableNumber > this.idUtils.getMaxTableNumber() ? tableNumber
                    - (this.idUtils.getMaxTableNumber() - this.idUtils.getMinTableNumber()) : tableNumber;
            final int id = this.idUtils.createGlobalId(schemaNumber, tableNumber);
            if (!this.isIdInUse(id)) {
                return id;
            }
        }
        throw new IllegalStateException(String.format("No free table ID left within schema %s.", schema));
    }

    @Override
    public boolean hasTargetWithId(int id) {
        return isIdInUse(id);
    }

    private boolean isIdInUse(final int id) {
        try {
            return this.sqlInterface.isTargetIdInUse(id);
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Could not determine if ID %s is in use.", id), e);
        }
    }

    @Override
    public void registerTargetObject(final Target target) {
        try {
            ((AbstractRDBMSTarget) target).store();
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public String toString() {
        return "MetadataStore[" + this.sqlInterface + "]";
    }

    @Override
    public Collection<ConstraintCollection<?>> getConstraintCollections() {
        if (this.constraintCollectionCache == null) {
            try {
                this.constraintCollectionCache = this.sqlInterface.getAllConstraintCollections();
            } catch (SQLException e) {
                throw new MetadataStoreException(e);
            }
        }
        return this.constraintCollectionCache;
    }

    @Override
    public ConstraintCollection<?> getConstraintCollection(int id) {
        try {
            return this.sqlInterface.getConstraintCollectionById(id);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    public SQLInterface getSQLInterface() {
        return this.sqlInterface;
    }

    @Override
    public int getUnusedConstraintCollectonId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    public <T> ConstraintCollection<T> createConstraintCollection(String userDefinedId,
                                                                  String description,
                                                                  Experiment experiment,
                                                                  Class<T> cls,
                                                                  Target... scope) {
        // Make sure that the given targets are actually compatible with this kind of metadata store.
        for (Target target : scope) {
            Validate.isAssignableFrom(AbstractRDBMSTarget.class, target.getClass());
        }
        ConstraintCollection<T> constraintCollection = new RDBMSConstraintCollection<>(
                this.getUnusedConstraintCollectonId(),
                userDefinedId,
                description,
                experiment,
                new HashSet<>(Arrays.asList(scope)),
                this.getSQLInterface(),
                cls
        );

        // Store the constraint collection in the DB.
        try {
            this.sqlInterface.addConstraintCollection(constraintCollection);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }

        if (this.constraintCollectionCache != null) this.constraintCollectionCache.add(constraintCollection);

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
        try {
            return this.sqlInterface.getSchemasByName(schemaName);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Schema getSchemaById(int schemaId) {
        try {
            return this.sqlInterface.getSchemaById(schemaId);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public void removeSchema(Schema schema) {
        try {
            this.flush();
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
            this.flush();
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    private void checkIfInScopeAndDelete(Target target) {
        for (ConstraintCollection<?> collection : this.getConstraintCollections()) {
            if (collection.getScope().contains(target)) {
                this.removeConstraintCollection(collection);
            }
        }
    }

    @Override
    public void removeConstraintCollection(ConstraintCollection<?> constraintCollection) {
        this.constraintCollectionCache = null;
        try {
            sqlInterface.removeConstraintCollection(constraintCollection);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    public void setUseJournal(boolean isUseJournal) {
        this.sqlInterface.setUseJournal(isUseJournal);
    }

    @Override
    public int getUnusedAlgorithmId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    @Override
    public int getUnusedExperimentId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    @Override
    public Algorithm createAlgorithm(String name) {
        RDBMSAlgorithm algorithm = new RDBMSAlgorithm(getUnusedAlgorithmId(), name, getSQLInterface());
        try {
            this.sqlInterface.addAlgorithm(algorithm);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
        return algorithm;
    }

    @Override
    public Algorithm getAlgorithmById(int algorithmId) {
        try {
            return this.sqlInterface.getAlgorithmByID(algorithmId);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Collection<Algorithm> getAlgorithms() {
        try {
            return this.sqlInterface.getAllAlgorithms();
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Collection<Experiment> getExperiments() {
        try {
            return this.sqlInterface.getAllExperiments();
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Experiment createExperiment(String description, Algorithm algorithm) {
        RDBMSExperiment experiment = new RDBMSExperiment(getUnusedExperimentId(), description, algorithm, this.sqlInterface);
        try {
            this.sqlInterface.writeExperiment(experiment);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
        return experiment;
    }

    @Override
    public void removeAlgorithm(Algorithm algorithm) {
        try {
            this.sqlInterface.removeAlgorithm(algorithm);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public void removeExperiment(Experiment experiment) {
        try {
            this.sqlInterface.removeExperiment(experiment);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Experiment getExperimentById(int experimentId) {
        try {
            return this.sqlInterface.getExperimentById(experimentId);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Algorithm getAlgorithmByName(String name) {
        try {
            return this.sqlInterface.getAlgorithmByName(name);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public void close() {
        try {
            sqlInterface.flush();
            sqlInterface.closeMetaDataStore();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
