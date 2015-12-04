package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.model.util.IdUtils.IdTypes;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import it.unimi.dsi.fastutil.ints.IntIterator;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * This class is a {@link de.hpi.isg.mdms.model.constraints.Constraint} representing the pattern of a certain {@link Column}. {@link Column}.
 */
public class PatternConstraint extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatternConstraint.class);

    // TODO: Update serializer. It is broken, because it still assumes the removed Constraintt table.
//    public static class PatternConstraintSQLiteSerializer implements ConstraintSQLSerializer<PatternConstraint> {
//
//        private final static String tableName = "Patternn";
//        private final static String tableNameEntry = "PatternEntryy";
//
//        /**
//         * Keeps track of currently maximum used maximum {@code constraintId} in the database.
//         */
//        private int currentConstraintIdMax = -1;
//
//        private final SQLInterface sqlInterface;
//
//        DatabaseWriter<Object[]> insertPatternConstraintWriter;
//
//        DatabaseWriter<Integer> deletePatternConstraintWriter;
//
//        DatabaseQuery<Void> queryPatternConstraints;
//
//        DatabaseQuery<Integer> queryPatternConstraintsForConstraintCollection;
//		private PreparedStatementBatchWriter<Object[]> insertPatternEntryConstraintWriter;
//		private PreparedStatementBatchWriter<Integer> deletePatternEntryConstraintWriter;
//
//        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_PATTERNCONSTRAINT_WRITER_FACTORY =
//                new PreparedStatementBatchWriter.Factory<>(
//                        "INSERT INTO " + tableName + " (constraintCollectionId, constraintId, columnId) VALUES (?, ?);",
//                        new PreparedStatementAdapter<Object[]>() {
//                            @Override
//                            public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
//                                    throws SQLException {
//                                preparedStatement.setInt(1, (Integer) parameters[0]);
//                                preparedStatement.setInt(2, (Integer) parameters[1]);
//                                preparedStatement.setInt(3, (Integer) parameters[2]);
//                            }
//                        },
//                        tableName);
//
//        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_PATTERNENTRYCONSTRAINT_WRITER_FACTORY =
//                new PreparedStatementBatchWriter.Factory<>(
//                        "INSERT INTO " + tableNameEntry + " (constraintId, patternn, count) VALUES (?, ?, ?);",
//                        new PreparedStatementAdapter<Object[]>() {
//                            @Override
//                            public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
//                                    throws SQLException {
//                                preparedStatement.setInt(1, (Integer) parameters[0]);
//                                preparedStatement.setString(2, String.valueOf(parameters[1]));
//                                preparedStatement.setInt(3, (Integer) parameters[2]);
//                            }
//                        },
//                        tableNameEntry);
//
//
//        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_PATTERNCONSTRAINT_WRITER_FACTORY =
//                new PreparedStatementBatchWriter.Factory<>(
//                        "DELETE from " + tableName
//                                + " where constraintId=?;",
//                        new PreparedStatementAdapter<Integer>() {
//                            @Override
//                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
//                                    throws SQLException {
//                                preparedStatement.setInt(1, parameter);
//                            }
//                        },
//                        tableName);
//
//        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_PATTERNENTRYCONSTRAINT_WRITER_FACTORY =
//                new PreparedStatementBatchWriter.Factory<>(
//                        "DELETE from " + tableNameEntry
//                                + " where constraintId=?;",
//                        new PreparedStatementAdapter<Integer>() {
//                            @Override
//                            public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
//                                    throws SQLException {
//                                preparedStatement.setInt(1, parameter);
//                            }
//                        },
//                        tableNameEntry);
//
//        private static final StrategyBasedPreparedQuery.Factory<Void> PATTERNCONSTRAINT_QUERY_FACTORY =
//                new StrategyBasedPreparedQuery.Factory<>(
//                        "SELECT Patternn.id as id, Patternn.columnId as columnId, PatternEntry.patternn as patternn, PatternEntry.count as count,"
//                                + " constraintt.constraintCollectionId as constraintCollectionId"
//                                + " from Patternn,PatternEntryy where Patternn.constraintId = constraintt.id"
//                                + " and PatternEntryy.constraintID = constraintt.id ;",
//                        PreparedStatementAdapter.VOID_ADAPTER,
//                        tableName);
//
//        private static final StrategyBasedPreparedQuery.Factory<Integer> PATTERNCONSTRAINT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
//                new StrategyBasedPreparedQuery.Factory<>(
//                        "SELECT constraintt.id as id, Patternn.columnId as columnId, PatternEntry.patternn as patternn, PatternEntry.count as count,"
//                                + " constraintt.constraintCollectionId as constraintCollectionId"
//                                + " from Patternn, constraintt, PatternEntryy where Patternn.constraintId = constraintt.id"
//                                + " and PatternEntryy.constraintID = constraintt.id "
//                                + " and constraintt.constraintCollectionId=?;",
//                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
//                        tableName);
//
//        public PatternConstraintSQLiteSerializer(SQLInterface sqlInterface) {
//            this.sqlInterface = sqlInterface;
//
//            try {
//                this.insertPatternConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
//                        INSERT_PATTERNCONSTRAINT_WRITER_FACTORY);
//
//                this.deletePatternConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
//                        DELETE_PATTERNCONSTRAINT_WRITER_FACTORY);
//
//                this.insertPatternEntryConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
//                        INSERT_PATTERNENTRYCONSTRAINT_WRITER_FACTORY);
//
//                this.deletePatternEntryConstraintWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
//                        DELETE_PATTERNENTRYCONSTRAINT_WRITER_FACTORY);
//
//                this.queryPatternConstraints = sqlInterface.getDatabaseAccess().createQuery(
//                        PATTERNCONSTRAINT_QUERY_FACTORY);
//
//                this.queryPatternConstraintsForConstraintCollection = sqlInterface.getDatabaseAccess().createQuery(
//                        PATTERNCONSTRAINT_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        @Override
//        public void serialize(Constraint patternConstraint, ConstraintCollection constraintCollection) {
//            Validate.isTrue(patternConstraint instanceof PatternConstraint);
//            ensureCurrentConstraintIdMaxInitialized();
//            Integer constraintId = ++this.currentConstraintIdMax;
//            try {
//                insertPatternConstraintWriter.write(new Object[] {
//                        constraintCollection.getId() ,patternConstraint
//                                .getTargetReference()
//                                .getAllTargetIds().iterator().nextInt()
//                });
//                for (String pattern: ((PatternConstraint) patternConstraint).getPatterns().keySet()){
//                	insertPatternEntryConstraintWriter.write(new Object[]{
//                			constraintId, pattern, ((PatternConstraint) patternConstraint).getPatterns().get(pattern)
//                	});
//                }
//
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//        }
//
//        private void ensureCurrentConstraintIdMaxInitialized() {
//            if (this.currentConstraintIdMax != -1) {
//                return;
//            }
//
//            try {
//                this.currentConstraintIdMax = 0;
//                try (ResultSet res = this.sqlInterface.getDatabaseAccess().query("SELECT MAX(constraintId) from " + getTableNames().get(0) + ";", getTableNames().get(0))) {
//                    while (res.next()) {
//                        if (this.currentConstraintIdMax < res.getInt("max(constraintId)")) {
//                            this.currentConstraintIdMax = res.getInt("max(constraintId)");
//                        }
//                    }
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        @Override
//        public Collection<PatternConstraint> deserializeConstraintsOfConstraintCollection(
//                ConstraintCollection constraintCollection) {
//            boolean retrieveConstraintCollection = constraintCollection == null;
//
//            HashMap<Integer, PatternConstraint> patternConstraints = new HashMap<Integer, PatternConstraint>();
//            HashMap<String,Integer> patternEntries;
//
//            try {
//                ResultSet rsPatternConstraints = retrieveConstraintCollection ?
//                        queryPatternConstraints.execute(null) : queryPatternConstraintsForConstraintCollection
//                                .execute(constraintCollection.getId());
//                while (rsPatternConstraints.next()) {
//                    if (retrieveConstraintCollection) {
//                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
//                                .getConstraintCollectionById(rsPatternConstraints
//                                        .getInt("constraintCollectionId"));
//                    }
//
//                    if(patternConstraints.containsKey(rsPatternConstraints.getInt("constraintID"))){
//                    	patternEntries = patternConstraints.get(rsPatternConstraints.getInt("constraintID")).getPatterns();
//                    	patternEntries.put(rsPatternConstraints.getString("patternn"), rsPatternConstraints.getInt("count"));
//                    }else{
//                    	patternEntries = new HashMap<String,Integer>();
//                    	patternEntries.put(rsPatternConstraints.getString("patternn"), rsPatternConstraints.getInt("count"));
//	                    patternConstraints
//	                            .put(rsPatternConstraints.getInt("constraintID"), PatternConstraint.build(
//	                                    new SingleTargetReference(this.sqlInterface.getColumnById(rsPatternConstraints
//	                                            .getInt("columnId")).getId()), constraintCollection, patternEntries));
//                    }
//                }
//                rsPatternConstraints.close();
//
//                return patternConstraints.values();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        @Override
//        public List<String> getTableNames() {
//            return Arrays.asList(tableName, tableNameEntry);
//        }
//
//        @Override
//        public void initializeTables() {
//            if (!sqlInterface.tableExists(tableName)) {
//                String createTable = "CREATE TABLE [" + tableName + "]\n" +
//                        "(\n" +
//                        "    [constraintId] integer NOT NULL,\n" +
//                        "    [columnId] integer NOT NULL,\n" +
//                        "    FOREIGN KEY ([constraintId])\n" +
//                        "    REFERENCES [Constraintt] ([id]),\n" +
//                        "    FOREIGN KEY ([columnId])\n" +
//                        "    REFERENCES [Columnn] ([id])\n" +
//                        ");";
//                this.sqlInterface.executeCreateTableStatement(createTable);
//            }
//            if (!sqlInterface.tableExists(tableNameEntry)) {
//                String createTable = "CREATE TABLE [" + tableNameEntry + "]\n" +
//                        "(\n" +
//                        "    [constraintId] integer NOT NULL,\n" +
//                        "    [patternn] text,\n" +
//                        "    [count] integer,\n" +
//                        "    FOREIGN KEY ([constraintId])\n" +
//                        "    REFERENCES [Constraintt] ([id])\n" +
//                        ");";
//                this.sqlInterface.executeCreateTableStatement(createTable);
//            }
//            if (!sqlInterface.tableExists(tableName) || !sqlInterface.tableExists(tableNameEntry)) {
//                throw new IllegalStateException("Not all tables necessary for serializer were created.");
//            }
//
//        }
//
//        @Override
//        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
//            try {
//                ResultSet rsTypeConstraints = queryPatternConstraintsForConstraintCollection
//                        .execute(constraintCollection.getId());
//                while (rsTypeConstraints.next()) {
//                    this.deletePatternConstraintWriter.write(rsTypeConstraints.getInt("id"));
//                    this.deletePatternEntryConstraintWriter.write(rsTypeConstraints.getInt("id"));
//                }
//                rsTypeConstraints.close();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    private static final long serialVersionUID = 3194245498846860560L;

    private final HashMap<String, Integer> patterns;

    private final TargetReference target;

    @Deprecated
    public static PatternConstraint build(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            HashMap<String, Integer> patterns) {
        PatternConstraint patternConstraint = new PatternConstraint(target, patterns);
        return patternConstraint;
    }

    public static PatternConstraint buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            HashMap<String, Integer> patterns) {
        PatternConstraint patternConstraint = new PatternConstraint(target, patterns);
        constraintCollection.add(patternConstraint);
        return patternConstraint;
    }

    public PatternConstraint(final SingleTargetReference target, HashMap<String, Integer> patterns) {
        Validate.isTrue(target.getAllTargetIds().size() == 1);

        this.patterns = patterns;
        this.target = target;
    }

    @Override
    public String toString() {
        return "PatternConstraint [pattern=" + patterns + "]";
    }

    @Override
    public TargetReference getTargetReference() {
        return target;
    }

    public HashMap<String, Integer> getPatterns() {
        return patterns;
    }

    @Override
    public ConstraintSQLSerializer<PatternConstraint> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
//            return new PatternConstraintSQLiteSerializer(sqlInterface);
            throw new RuntimeException("To do: PatternConstraintSQLiteSerializer is not yet available.");
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }
}
