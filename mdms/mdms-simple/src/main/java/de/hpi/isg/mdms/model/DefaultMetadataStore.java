package de.hpi.isg.mdms.model;

import de.hpi.isg.mdms.exceptions.IdAlreadyInUseException;
import de.hpi.isg.mdms.exceptions.MetadataStoreNotFoundException;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.constraints.DefaultConstraintCollection;
import de.hpi.isg.mdms.model.experiment.*;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.AbstractTarget;
import de.hpi.isg.mdms.model.targets.DefaultSchema;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * The default in-memory implementation of the {@link de.hpi.isg.mdms.model.MetadataStore}.
 *
 */

public class DefaultMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataStore.class);

    /**
     * Creates a new DefaultMetadataStore and saves it to disk.
     * @param file stores the metadata store
     * @return the DefaultMetadataStore
     * @throws IOException
     */
    public static DefaultMetadataStore createAndSave(final File file) throws IOException {
        return createAndSave(file, IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    /**
     * Creates a new DefaultMetadataStore and saves it to disk.
     * @param file stores the metadata store
     * @param numTableBitsInIds is the number of bits in object IDs to use for tables
     * @param numColumnBitsInIds is the number of bits in object IDs to use for columns
     * @return the DefaultMetadataStore
     * @throws IOException
     */
    public static DefaultMetadataStore createAndSave(final File file, int numTableBitsInIds,
                                                                         int numColumnBitsInIds) throws IOException {

        final DefaultMetadataStore metadataStore = new DefaultMetadataStore(file, numTableBitsInIds, numColumnBitsInIds);
        if (!file.exists()) {
            file.createNewFile();
        }
        try {
            metadataStore.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return metadataStore;
    }

    /**
     * Loads a DefaultMetadataStore from the given file.
     * @param file is the file that contains the metadata store
     * @return the loaded metadata store
     * @throws MetadataStoreNotFoundException if no metadata store could be loaded from the given file
     */
    public static DefaultMetadataStore load(final File file) throws MetadataStoreNotFoundException {

        FileInputStream fin;
        try {
            fin = new FileInputStream(file);
            final ObjectInputStream ois = new ObjectInputStream(fin);
            final DefaultMetadataStore metadataStore = (DefaultMetadataStore) ois.readObject();
            ois.close();
            metadataStore.setStoreLocation(file);
            return metadataStore;
        } catch (IOException | ClassNotFoundException e) {
            throw new MetadataStoreNotFoundException(e);
        }

    }

    private final Collection<Schema> schemas;

    private final Collection<Algorithm> algorithms;
    
    private final Collection<Experiment> experiments;
    
    private final Collection<ConstraintCollection> constraintCollections;

    private final Int2ObjectMap<Target> allTargets;

    transient private File storeLocation;

    @ExcludeHashCodeEquals
    private final IdUtils idUtils;

    @ExcludeHashCodeEquals
    private final Random randomGenerator = new Random();

    public DefaultMetadataStore() {
        this(null, 12, 12);
    }

    public DefaultMetadataStore(File location, int numTableBitsInIds, int numColumnBitsInIds) {
        this.storeLocation = location;

        this.schemas = Collections.synchronizedSet(new HashSet<Schema>());
        this.constraintCollections = Collections.synchronizedList(new LinkedList<ConstraintCollection>());
        this.allTargets = new Int2ObjectOpenHashMap<>();
        this.idUtils = new IdUtils(numTableBitsInIds, numColumnBitsInIds);
        this.experiments = Collections.synchronizedList(new LinkedList<Experiment>());
        this.algorithms = Collections.synchronizedList(new LinkedList<Algorithm>());
    }

    @Override
    public Schema addSchema(final String name, String description, final Location location) {
        final int id = this.getUnusedSchemaId();
        final Schema schema = DefaultSchema.buildAndRegister(this, id, name, description, location);
        this.schemas.add(schema);
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
        final List<Schema> results = new ArrayList<>();
        for (final Schema schema : this.schemas) {
            if (schema.getName().equals(schemaName)) {
                results.add(schema);
            }
        }
        if (results.size() > 1) {
            throw new NameAmbigousException(schemaName);
        }
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    @Override
    public Collection<Schema> getSchemas() {
        return this.schemas;
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
    public int getUnusedConstraintCollectonId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
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
    public int getUnusedTableId(final Schema schema) {
        Validate.isTrue(this.schemas.contains(schema));
        final int schemaNumber = this.idUtils.getLocalSchemaId(schema.getId());
        final int searchOffset = schema.getTables().size();
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
        synchronized (this.allTargets) {
            return this.allTargets.containsKey(id);
        }
    }

    private boolean idIsInUse(final int id) {
        // delegate
        return hasTargetWithId(id);
    }

    @Override
    public void registerTargetObject(final Target message) {
        synchronized (this.allTargets) {
            Target oldTarget = this.allTargets.put(message.getId(), message);
            if (oldTarget != null) {
                this.allTargets.put(oldTarget.getId(), oldTarget);
                throw new IdAlreadyInUseException("Id is already in use: " + message.getId());
            }
        }
    }

    /**
     * @param storeLocation
     *        the storeLocation to set
     */
    public void setStoreLocation(File storeLocation) {
        this.storeLocation = storeLocation;
    }

    @Override
    public String toString() {
        return "MetadataStore[" + this.schemas.size() + " schemas, " + this.constraintCollections.size()
                + " constraint collections]";
    }

    @Override
    public Collection<ConstraintCollection> getConstraintCollections() {
        return this.constraintCollections;
    }

    @Override
    public ConstraintCollection getConstraintCollection(int id) {
        throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public ConstraintCollection createConstraintCollection(String description, Target... scope) {
        // Make sure that the given targets are actually compatible with this kind of metadata store.
        for (Target target : scope) {
            Validate.isAssignableFrom(AbstractTarget.class, target.getClass());
        }
        ConstraintCollection constraintCollection = new DefaultConstraintCollection(this,
                getUnusedConstraintCollectonId(),
                new HashSet<Constraint>(), new HashSet<Target>(Arrays.asList(scope)));
        constraintCollection.setDescription(description);
        this.constraintCollections.add(constraintCollection);
        return constraintCollection;
    }

	@Override
	public ConstraintCollection createConstraintCollection(String description,
			Experiment experiment, Target... scope) {
        // Make sure that the given targets are actually compatible with this kind of metadata store.
        for (Target target : scope) {
            Validate.isAssignableFrom(AbstractTarget.class, target.getClass());
        }
        ConstraintCollection constraintCollection = new DefaultConstraintCollection(this,
                getUnusedConstraintCollectonId(),
                new HashSet<Constraint>(), new HashSet<Target>(Arrays.asList(scope)), experiment);
        this.constraintCollections.add(constraintCollection);
        experiment.add(constraintCollection);
        return constraintCollection;
	}

    
    
    @Override
    public IdUtils getIdUtils() {
        return this.idUtils;
    }

    @Override
    public void save(String path) throws IOException {
        File file = new File(path);
        this.storeLocation = file;
        saveToDefaultLocation();
    }

    private void saveToDefaultLocation() throws FileNotFoundException, IOException {
        this.storeLocation.getParentFile().mkdirs();
        final FileOutputStream fout = new FileOutputStream(this.storeLocation);
        final ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(this);
        oos.close();
    }

    @Override
    public void flush() throws Exception {
        if (this.storeLocation == null) {
            LOGGER.warn("Cannot flush metadata store because it has no default saving location.");
        } else {
            saveToDefaultLocation();
        }
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) {
        // TODO Implement method.
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Schema getSchemaById(int schemaId) {
        // TODO Implement method.
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeSchema(Schema schema) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not supported yet.");
        //
    }

    @Override
    public void removeConstraintCollection(ConstraintCollection constraintCollection) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not supported yet.");
        //
    }

	@Override
	public Algorithm createAlgorithm(String name) {
		final int id = this.getUnusedAlgorithmId();
        final Algorithm algorithm = new DefaultAlgorithm (id, name, new HashSet<Experiment>());
        this.algorithms.add(algorithm);
        return algorithm;
	}

	@Override
	public Algorithm getAlgorithmById(int algorithmId) {
		for (Algorithm algorithm : this.algorithms){
			if (algorithm.getId() == algorithmId){
				return algorithm;
			}
		}
		return null;
	}

	@Override
	public Experiment getExperimentById(int experimentId) {
		for (Experiment experiment : this.experiments){
			if (experiment.getId() == experimentId){
				return experiment;
			}
		}
		return null;
	}
	
	@Override
	public Algorithm getAlgorithmByName(String name) {
		for (Algorithm algorithm : algorithms){
			if (algorithm.getName() == name){
				return algorithm;
			}
		}
		return null;
	}

	@Override
	public Collection<Algorithm> getAlgorithms() {
		return this.algorithms;
	}
	
	
	@Override
	public Experiment createExperiment(String description, Algorithm algorithm) {
		final int id = this.getUnusedExperimentId();
        final Experiment experiment = new DefaultExperiment (this, id, algorithm, new HashSet<ConstraintCollection>(), new HashMap<String, String>(), new HashSet<Annotation>());
        this.experiments.add(experiment);
        algorithm.addExperiment(experiment);
        return experiment;
	}

	@Override
	public void removeAlgorithm(Algorithm algorithm) {
		this.algorithms.remove(algorithm);
	}

	@Override
	public void removeExperiment(Experiment experiment) {
		this.experiments.remove(experiment);
	}

	@Override
	public Collection<Experiment> getExperiments() {
        return this.experiments;
    }

    @Override
	public void close() {
		try {
			this.flush();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

}
