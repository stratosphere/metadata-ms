package de.hpi.isg.mdms.flink;


import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.flink.readwrite.FlinkMetdataStoreAdapter;
import de.hpi.isg.mdms.flink.serializer.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

@Ignore("The integration with RDBMSMetadataStore broke due to schema changes there.")
public class FlinkRetrieveConstraintTest {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private File testDb;
    private Connection connection;
    private MetadataStore store;
    private Column col1;
    private Column col2;
    private ExecutionEnvironment flinkExecutionEnvironment;

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        this.logger.info("setUp() started.");

        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());

        this.store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

        final Schema dummySchema1 = store.addSchema("PDB", null, new DefaultLocation());
        col1 = dummySchema1.addTable(store, "table1", null, new DefaultLocation()).addColumn(store,
                "foo", null, 1);
        col2 = dummySchema1.addTable(store, "table1", null, new DefaultLocation()).addColumn(store,
                "bar", null, 2);


        Configuration configuration = new Configuration();
        configuration.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 128);
        configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
        this.flinkExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment(configuration);
    }

    @After
    public void tearDown() {
        this.flinkExecutionEnvironment = null;
        store.close();
        this.testDb.delete();
    }

    @Test
    public void testDistinctValueCount() throws Exception {

        //write dvcs to database
        ConstraintCollection<DistinctValueCount> cc = store.createConstraintCollection(null, DistinctValueCount.class, store.getTargetByName("PDB"));
        cc.add(new DistinctValueCount(this.col1.getId(), 1));
        cc.add(new DistinctValueCount(this.col2.getId(), 3));

        this.store.flush();

        //flink job
        DataSet<Tuple> constraints = FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.flinkExecutionEnvironment,
                this.store,
                cc,
                new DVCFlinkSerializer());

        List<Tuple2<Integer, Integer>> outData = new ArrayList<>();
        constraints.output(new LocalCollectionOutputFormat(outData));

        this.flinkExecutionEnvironment.execute("Distinct Value Count Reading");

        assertTrue(outData.size() == 2);
    }

    @Test
    public void testDistinctValueOverlap() throws Exception {
        ConstraintCollection<DistinctValueOverlap> cc = store.createConstraintCollection(null, DistinctValueOverlap.class, store.getTargetByName("PDB"));
        cc.add(new DistinctValueOverlap(2, this.col1.getId(), this.col2.getId()));
        this.store.flush();

        //flink job
        DataSet<Tuple> constraints = FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.flinkExecutionEnvironment,
                this.store,
                cc,
                new DVOFlinkSerializer());

        List<Tuple3<Integer, Integer, Integer>> outData = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        constraints.output(new LocalCollectionOutputFormat(outData));

        this.flinkExecutionEnvironment.execute("Distinct Value Overlap Reading");

        assertTrue(outData.size() == 1);
    }


    @Test
    public void testInclusionDependency() throws Exception {
        ConstraintCollection<InclusionDependency> cc = store.createConstraintCollection(null, InclusionDependency.class, store.getTargetByName("PDB"));
        int[] referenced = {col1.getId()};
        int[] dependent = {col2.getId()};
        cc.add(new InclusionDependency(dependent, referenced));
        this.store.flush();

        //flink job
        DataSet<Tuple> constraints = FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.flinkExecutionEnvironment,
                this.store,
                cc,
                new INDFlinkSerializer());

        List<Tuple2<int[], int[]>> outData = new ArrayList<Tuple2<int[], int[]>>();
        constraints.output(new LocalCollectionOutputFormat(outData));

        this.flinkExecutionEnvironment.execute("Inclusion Dependency Reading");

        assertTrue(outData.size() == 1);
    }

    @Test
    public void testUniqueColumnCombination() throws Exception {
        ConstraintCollection<UniqueColumnCombination> cc = store.createConstraintCollection(null, UniqueColumnCombination.class, store.getTargetByName("PDB"));
        int[] columns = {col1.getId()};
        cc.add(new UniqueColumnCombination(columns));
        this.store.flush();

        //flink job
        DataSet<Tuple> constraints = FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.flinkExecutionEnvironment,
                this.store,
                cc,
                new UCCFlinkSerializer());

        List<Tuple2<int[], int[]>> outData = new ArrayList<Tuple2<int[], int[]>>();
        constraints.output(new LocalCollectionOutputFormat(outData));

        this.flinkExecutionEnvironment.execute("Unique Column Combination Reading");

        assertTrue(outData.size() == 1);

    }

    @Test
    public void testFunctionalDependency() throws Exception {
        ConstraintCollection<FunctionalDependency> cc = store.createConstraintCollection(null, FunctionalDependency.class, store.getTargetByName("PDB"));
        int[] lhs = {col1.getId()};
        int rhs = col2.getId();
        cc.add(new FunctionalDependency(lhs, rhs));
        this.store.flush();

        //flink job
        DataSet<Tuple> constraints = FlinkMetdataStoreAdapter.getConstraintsFromCollection(
                this.flinkExecutionEnvironment,
                this.store,
                cc,
                new FDFlinkSerializer());

        List<Tuple2<int[], int[]>> outData = new ArrayList<Tuple2<int[], int[]>>();
        constraints.output(new LocalCollectionOutputFormat(outData));

        this.flinkExecutionEnvironment.execute("Functional Dependency Reading");

        assertTrue(outData.size() == 1);

    }


}
