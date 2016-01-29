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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;

import static org.junit.Assert.*;


public class FlinkWriteConstraintTest {

    private File testDb;
    private Connection connection;
    private MetadataStore store;
    private Column col1;
    private Column col2;
    private ConstraintCollection dummyConstraintCollection;

    @Before
    public void setUp() {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

        this.store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

        final Schema dummySchema1 = store.addSchema("PDB", null, new DefaultLocation());
        col1 = dummySchema1.addTable(store, "table1", null, new DefaultLocation()).addColumn(store,
                "foo", null, 1);
        col2 = dummySchema1.addTable(store, "table1", null, new DefaultLocation()).addColumn(store,
                "bar", null, 2);

        dummyConstraintCollection = store.createConstraintCollection(null, dummySchema1);

    }

    @After
    public void tearDown() {
        store.close();
        this.testDb.delete();
    }


    @Test
    public void testDistinctValueCount() throws Exception {

        ArrayList<Tuple2<Integer, Integer>> dvcs = new ArrayList<Tuple2<Integer, Integer>>();
        dvcs.add(new Tuple2<Integer, Integer>(col1.getId(), 1));
        dvcs.add(new Tuple2<Integer, Integer>(col2.getId(), 2));

        //flink job
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Tuple2<Integer, Integer>> distinctValueCounts = env.fromCollection(dvcs);
        FlinkMetdataStoreAdapter.save(distinctValueCounts, dummyConstraintCollection, new DVCFlinkSerializer());

        env.execute("Distinct Value Count Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollection(dummyConstraintCollection.getId()).getConstraints().size() == dvcs.size());
        assertTrue(((DistinctValueCount) store.getConstraintCollections().iterator().next().getConstraints().iterator().next()).getNumDistinctValues() == dvcs.get(0).f1);
    }

    @Test
    public void testDistinctValueOverlap() throws Exception {

        ArrayList<Tuple3<Integer, Integer, Integer>> dvos = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        dvos.add(new Tuple3<Integer, Integer, Integer>(col1.getId(), col2.getId(), 2));

        //flink job
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Tuple3<Integer, Integer, Integer>> distinctValueOverlaps = env.fromCollection(dvos);

        FlinkMetdataStoreAdapter.save(distinctValueOverlaps, dummyConstraintCollection, new DVOFlinkSerializer());

        env.execute("Distinct Value Overlap Writing");


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
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<int[], int[]>> inclusionDependencies = env.fromCollection(inds);

        FlinkMetdataStoreAdapter.save(inclusionDependencies, dummyConstraintCollection, new INDFlinkSerializer());

        env.execute("IND Writing");

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
        ArrayList<Tuple1<int[]>> uccs = new ArrayList<Tuple1<int[]>>();
        uccs.add(new Tuple1<int[]>(columns));

        //flink job
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple1<int[]>> uniqueColumnCombinations = env.fromCollection(uccs);

        FlinkMetdataStoreAdapter.save(uniqueColumnCombinations, dummyConstraintCollection, new UCCFlinkSerializer());

        env.execute("UCC Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollections().iterator().next().getConstraints().size() == uccs.size());
        assertTrue(((UniqueColumnCombination) store.getConstraintCollections().iterator().next().getConstraints().iterator().next())
                .getTargetReference().getAllTargetIds().contains(uccs.get(0).f0[0]));

    }

    @Test
    public void testFunctionalDependency() throws Exception {

        ArrayList<Tuple2<int[], Integer>> fds = new ArrayList<Tuple2<int[], Integer>>();
        int[] referenced = {col1.getId()};
        int dependent = col2.getId();
        fds.add(new Tuple2<int[], Integer>(referenced, dependent));

        //flink job
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<int[], Integer>> functionalDependencies = env.fromCollection(fds);

        FlinkMetdataStoreAdapter.save(functionalDependencies, dummyConstraintCollection, new FDFlinkSerializer());

        env.execute("FD Writing");

        assertEquals(store.getConstraintCollection(dummyConstraintCollection.getId()), dummyConstraintCollection);
        assertTrue(store.getConstraintCollections().iterator().next().getConstraints().size() == fds.size());
        assertTrue(((FunctionalDependency) store.getConstraintCollections().iterator().next().getConstraints().iterator().next())
                .getTargetReference().getRHSTarget() == fds.get(0).f1);

    }


}
