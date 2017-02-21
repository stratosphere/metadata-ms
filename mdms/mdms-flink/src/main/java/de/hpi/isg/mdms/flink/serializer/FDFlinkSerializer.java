package de.hpi.isg.mdms.flink.serializer;


import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.flink.readwrite.SqLiteJDBCInputFormat;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class FDFlinkSerializer implements AbstractFlinkSerializer<FunctionalDependency, Tuple2<int[], Integer>> {

    private class AddFunctionalDependencyCommand implements Runnable {

        private int[] lhs;
        private Integer rhs;
        private ConstraintCollection<FunctionalDependency> constraintCollection;

        public AddFunctionalDependencyCommand(final Tuple2<int[], Integer> tuple, final ConstraintCollection<FunctionalDependency> constraintCollection) {
            super();
            this.lhs = tuple.f0;
            this.rhs = tuple.f1;
            this.constraintCollection = constraintCollection;
        }

        @Override
        public void run() {
            synchronized (this.constraintCollection) {
                FunctionalDependency fd = new FunctionalDependency(this.lhs, this.rhs);
                this.constraintCollection.add(fd);
            }
        }
    }


    @Override
    public DataSet<Tuple2<int[], Integer>> getConstraintsFromCollection(
            ExecutionEnvironment executionEnvironment,
            MetadataStore metadataStore,
            ConstraintCollection<? extends Constraint> datasourceCollection) {

        // Read data from a relational database using the JDBC input format
        RDBMSMetadataStore rdbms = (RDBMSMetadataStore) metadataStore;

        DataSet<Tuple3<Integer, Integer, Integer>> dbData =
                executionEnvironment.createInput(
                        // create and configure input format
                        SqLiteJDBCInputFormat.buildJDBCInputFormat()
                                .setDrivername("org.sqlite.JDBC")
                                .setDBUrl(rdbms.getSQLInterface().getDatabaseURL())
                                .setQuery("select FD.id, lhs_col, rhs_col from FD, FD_LHS "
                                        + "where FD.id = FD_LHS.constraintId and FD.constraintCollectionId = " + datasourceCollection.getId() + ";")
                                .finish(),
                        // specify type information for DataSet
                        new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
                );

        DataSet<Tuple2<int[], Integer>> fds = dbData.groupBy(0)
                .reduceGroup(new CreateFDs());

        return fds;

    }

    @SuppressWarnings("serial")
    private static final class CreateFDs implements GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<int[], Integer>> {

        @Override
        public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> values,
                           Collector<Tuple2<int[], Integer>> out) throws Exception {

            Integer rhs = null;
            ArrayList<Integer> lhs = new ArrayList<Integer>();
            for (Tuple3<Integer, Integer, Integer> tuple : values) {
                lhs.add(tuple.f1);
                rhs = tuple.f2;
            }
            out.collect(new Tuple2<int[], Integer>(
                    ArrayUtils.toPrimitive(lhs.toArray(new Integer[0])),
                    rhs));
        }

    }

    @Override
    public Runnable getAddRunnable(Tuple2<int[], Integer> tuple,
                                   ConstraintCollection<FunctionalDependency> constraintCollection) {
        return new AddFunctionalDependencyCommand(tuple, constraintCollection);
    }

}



