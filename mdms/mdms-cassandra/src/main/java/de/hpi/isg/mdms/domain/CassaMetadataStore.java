package de.hpi.isg.mdms.domain;

import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.rdbms.util.LocationCache;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;

import org.apache.commons.io.IOUtils;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * The cassandra implementation of the {@link MetadataStore} for storing the data inside of a cassandra database.
 */

public class CassaMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final String NUM_COLUMN_BITS_IN_IDS_KEY = "numColumnBitsInIds";

    private static final String NUM_TABLE_BITS_IN_IDS_KEY = "numTableBitsInIds";

    private static final long serialVersionUID = 400271996998552017L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassaMetadataStore.class);

	private static final String SETUP_SCRIPT_RESOURCE_PATH = "/schema_creation.cql";;

    
	static Cluster cluster;
	static Session session;
	
	static String host = "localhost";

    @ExcludeHashCodeEquals
    transient final Random randomGenerator = new Random();

    @ExcludeHashCodeEquals
    transient final IdUtils idUtils;

    @ExcludeHashCodeEquals
    transient final LocationCache locationCache = new LocationCache();

    public static CassaMetadataStore createNewInstance(String host) {
        return createNewInstance(host, IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    public static CassaMetadataStore createNewInstance(String host, int numTableBitsInIds,
            int numColumnBitsInIds) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(NUM_TABLE_BITS_IN_IDS_KEY, String.valueOf(numTableBitsInIds));
        configuration.put(NUM_COLUMN_BITS_IN_IDS_KEY, String.valueOf(numColumnBitsInIds));
        return CassaMetadataStore.createNewInstance(host, configuration);
    }

    public static CassaMetadataStore createNewInstance(String host,
            Map<String, String> configuration) {
    	
		cluster = Cluster.builder().addContactPoint(host).withPort(9042)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
				.build();

		cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(3000);
		for ( KeyspaceMetadata keyspace : cluster.getMetadata().getKeyspaces()) {
			if (keyspace.getName().equals("metadatastore")){
				LOGGER.warn("The metadata store will be overwritten.");
				session = cluster.connect();
				session.execute("DROP KEYSPACE metadatastore");
			};
		}
		
		 try {
			String cqlCreateTables = loadResource(SETUP_SCRIPT_RESOURCE_PATH);
			session = cluster.connect();

			if (host.equals("localhost")){
				session.execute("CREATE KEYSPACE metadatastore WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");	
			} else {
				session.execute("CREATE KEYSPACE metadatastore WITH replication = {'class':'NetworkTopologyStrategy', 'DC1':1};");				
			}
			
			
			session = cluster.connect("metadatastore");
			String[] statements = cqlCreateTables.split(";");
			for (String statement : statements){
				session.execute(statement);				
			}
		 } catch (IOException e) {
			 System.out.println("exception");
			e.printStackTrace();
		}
		session = cluster.connect("metadatastore");
        CassaMetadataStore metadataStore = new CassaMetadataStore(host);
        return metadataStore;
    }

    public static CassaMetadataStore load(String host) {
    	cluster = Cluster.builder().addContactPoint(host)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
				.build();
    	session = cluster.connect("metadatastore");

    	CassaMetadataStore metadataStore = new CassaMetadataStore(host);
        metadataStore.fillLocationCache();
        return metadataStore;
    }

    private CassaMetadataStore(String host) {
    	this.host = host;
    	this.cluster = Cluster.builder().addContactPoint(host).withPort(9042)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
				.build();
    	this.cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(1000);
    	this.session = cluster.connect("metadatastore");
        this.idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS);
    }

    @SuppressWarnings("unchecked")
    private void fillLocationCache() {
    	//TODO: implement
    }

    @Override
    public Schema addSchema(final String name, final String description, final Location location) {
        //TODO
    	return null;
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
        //TODO
    	return null;
    }

    @Override
    public Collection<Schema> getSchemas() {
        //TODO
    	return null;
    }

    @Override
    public int getUnusedSchemaId() {
        //TODO
    	return 0;
    }

    @Override
    public int getUnusedAlgorithmId() {
        return 0;
    }

    @Override
    public int getUnusedExperimentId() {
        return 0;
    }

    @Override
    public int getUnusedTableId(final Schema schema) {
        //TODO
    	return 0;
    }

    @Override
    public boolean hasTargetWithId(int id) {
        return idIsInUse(id);
    }

    private boolean idIsInUse(final int id) {
        //TODO
    	return false;
    }

    @Override
    public void registerTargetObject(final Target target) {
        // Register the Location type of the target.
    	//TODO
    }

    @Override
    public String toString() {
        return "MetadataStore[" + this.host + "]";
    }

    @Override
    public Collection<ConstraintCollection> getConstraintCollections() {
    	//TODO
    	return null;
    }

    @Override
    public ConstraintCollection getConstraintCollection(int id) {
    	//TODO
    	return null;
    }


    @Override
    public int getUnusedConstraintCollectonId() {
        return this.randomGenerator.nextInt(Integer.MAX_VALUE);
    }

    @Override
    public Algorithm createAlgorithm(String name) {
        return null;
    }

    @Override
    public Algorithm getAlgorithmById(int algorithmId) {
        return null;
    }

    @Override
    public Algorithm getAlgorithmByName(String name) {
        return null;
    }

    @Override
    public Collection<Algorithm> getAlgorithms() {
        return null;
    }

    @Override
    public Collection<Experiment> getExperiments() {
        return null;
    }

    @Override
    public Experiment createExperiment(String description, Algorithm algorithm) {
        return null;
    }

    @Override
    public ConstraintCollection<? extends Constraints> createConstraintCollection(String description, Experiment experiment, Class<T> , Target... scope) {
        return null;
    }

    @Override
    public ConstraintCollection createConstraintCollection(String description, Target... scope) {
    	//TODO
    	return null;
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
    	//TODO
    }

    @Override
    public Collection<Schema> getSchemasByName(String schemaName) {
    	//TODO
    	return null;
    }

    @Override
    public Schema getSchemaById(int schemaId) {
    	//TODO
    	return null;
    }

    @Override
    public void removeSchema(Schema schema) {
        //TODO
    }


    @Override
    public void removeConstraintCollection(ConstraintCollection constraintCollection) {
       //TODO
    }

    /**
     * Loads the given resource as String.
     *
     * @param resourcePath is the path of the resource
     * @return a {@link String} with the contents of the resource
     * @throws java.io.IOException
     */
    static String loadResource(String resourcePath) throws IOException {
        try (InputStream resourceStream = CassaMetadataStore.class.getResourceAsStream(resourcePath)) {
            return IOUtils.toString(resourceStream, "UTF-8");
        }
    }

	@Override
	public void close() {
		CassaMetadataStore.cluster.close();
		CassaMetadataStore.session.close();

		
	}

    @Override
    public void removeAlgorithm(Algorithm algorithm) {

    }

    @Override
    public void removeExperiment(Experiment experiment) {

    }

    @Override
    public Experiment getExperimentById(int experimentId) {
        return null;
    }


}
