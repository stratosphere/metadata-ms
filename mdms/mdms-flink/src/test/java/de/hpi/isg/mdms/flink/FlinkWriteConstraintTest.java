package de.hpi.isg.mdms.flink;


import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.flink.serializer.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.*;


public class FlinkWriteConstraintTest {

    private File testDb;
    private Connection connection;
    private MetadataStore store;
    private Column col1;
    private Column col2;
    private ConstraintCollection dummyConstraintCollection;
    private ExecutionEnvironment flinkExecutionEnvironment;

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
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

        dummyConstraintCollection = store.createConstraintCollection(null, dummySchema1);

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

        ArrayList<Tuple2<Integer, Integer>> dvcs = new ArrayList<>();
        dvcs.add(new Tuple2<>(col1.getId(), 1));
        dvcs.add(new Tuple2<>(col2.getId(), 2));

        //flink job
        DataSet<Tuple2<Integer, Integer>> distinctValueCounts = this.flinkExecutionEnvironment.fromCollection(dvcs);
        FlinkMetdataStoreAdapter.save(distinctValueCounts, dummyConstraintCollection, new DVCFlinkSerializer());

        this.flinkExecutionEnvironment.execute("Distinct Value Count Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollection(dummyConstraintCollection.getId()).getConstraints().size() == dvcs.size());
        assertTrue(((DistinctValueCount) store.getConstraintCollections().iterator().next().getConstraints().iterator().next()).getNumDistinctValues() == dvcs.get(0).f1);
    }

    @Test
    public void testDistinctValueOverlap() throws Exception {

        ArrayList<Tuple3<Integer, Integer, Integer>> dvos = new ArrayList<>();
        dvos.add(new Tuple3<>(col1.getId(), col2.getId(), 2));

        //flink job
        DataSet<Tuple3<Integer, Integer, Integer>> distinctValueOverlaps = this.flinkExecutionEnvironment.fromCollection(dvos);

        FlinkMetdataStoreAdapter.save(distinctValueOverlaps, dummyConstraintCollection, new DVOFlinkSerializer());

        this.flinkExecutionEnvironment.execute("Distinct Value Overlap Writing");


        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollection(dummyConstraintCollection.getId()).getConstraints().size() == dvos.size());
        assertTrue(((DistinctValueOverlap) store.getConstraintCollections().iterator().next().getConstraints().iterator().next()).getTargetReference().getAllTargetIds().contains(dvos.get(0).f0));
    }


    @Test
    public void testInclusionDependency() throws Exception {

        ArrayList<Tuple2<int[], int[]>> inds = new ArrayList<Tuple2<int[], int[]>>();
        int[] referenced = {col1.getId()};
        int[] dependent = {col2.getId()};
        inds.add(new Tuple2<>(dependent, referenced));

        //flink job
        DataSet<Tuple2<int[], int[]>> inclusionDependencies = this.flinkExecutionEnvironment.fromCollection(inds);

        FlinkMetdataStoreAdapter.save(inclusionDependencies, dummyConstraintCollection, new INDFlinkSerializer());

        this.flinkExecutionEnvironment.execute("IND Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollections().iterator().next().getConstraints().size() == inds.size());
        final int[] referencedColumns = ((InclusionDependency) store.getConstraintCollections().iterator().next().getConstraints().iterator().next())
                .getTargetReference().getReferencedColumns();
        assertArrayEquals(referencedColumns, referenced);
        final int[] dependentColumns = ((InclusionDependency) store.getConstraintCollections().iterator().next().getConstraints().iterator().next())
                .getTargetReference().getDependentColumns();
        assertArrayEquals(dependentColumns, dependent);

    }

    @Test
    public void testUniqueColumnCombination() throws Exception {

        int[] columns = {col1.getId()};
        ArrayList<Tuple1<int[]>> uccs = new ArrayList<>();
        uccs.add(new Tuple1<>(columns));

        //flink job
        DataSet<Tuple1<int[]>> uniqueColumnCombinations = this.flinkExecutionEnvironment.fromCollection(uccs);

        FlinkMetdataStoreAdapter.save(uniqueColumnCombinations, dummyConstraintCollection, new UCCFlinkSerializer());

        this.flinkExecutionEnvironment.execute("UCC Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollections().iterator().next().getConstraints().size() == uccs.size());
        assertTrue(((UniqueColumnCombination) store.getConstraintCollections().iterator().next().getConstraints().iterator().next())
                .getTargetReference().getAllTargetIds().contains(uccs.get(0).f0[0]));

    }

    @Test
    public void testFunctionalDependency() throws Exception {

        ArrayList<Tuple2<int[], Integer>> fds = new ArrayList<>();
        int[] referenced = {col1.getId()};
        int dependent = col2.getId();
        fds.add(new Tuple2<>(referenced, dependent));

        //flink job
        DataSet<Tuple2<int[], Integer>> functionalDependencies = this.flinkExecutionEnvironment.fromCollection(fds);

        FlinkMetdataStoreAdapter.save(functionalDependencies, dummyConstraintCollection, new FDFlinkSerializer());

        this.flinkExecutionEnvironment.execute("FD Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollections().iterator().next().getConstraints().size() == fds.size());
        assertTrue(((FunctionalDependency) store.getConstraintCollections().iterator().next().getConstraints().iterator().next())
                .getTargetReference().getRHSTarget() == fds.get(0).f1);

    }


}
