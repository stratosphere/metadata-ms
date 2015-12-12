package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static de.hpi.isg.mdms.domain.util.SQLiteConstraintUtils.getNullableDouble;


/**
 * This constraint class encapsulates string-specific single column statistics.
 */
public class NumberColumnStatistics implements RDBMSConstraint {

    /**
     * Special values in a column.
     */
    private double minValue = Double.NaN, maxValue = Double.NaN;

    /**
     * The standard deviation of values in a column.
     */
    private double standardDeviation = Double.NaN;

    /**
     * The average of values in a column.
     */
    private double average = Double.NaN;

    /**
     * Reference to the described column.
     */
    private final SingleTargetReference targetReference;

    /**
     * Create a new instance.
     *
     * @param columnId the ID of the column that is to be described by the new instance
     */
    public NumberColumnStatistics(int columnId) {
        this.targetReference = new SingleTargetReference(columnId);
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.targetReference;
    }

    @Override
    public ConstraintSQLSerializer<NumberColumnStatistics> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new SQLiteSerializer((SQLiteInterface) sqlInterface);
        }
        throw new RuntimeException("No serializer available for " + sqlInterface);
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    /**
     * SQLite serializer for {@link NumberColumnStatistics}.
     */
    public static class SQLiteSerializer implements ConstraintSQLSerializer<NumberColumnStatistics> {

        /**
         * Name of the SQL table to store the {@link NumberColumnStatistics} instances.
         */
        private static final String TABLE_NAME = "numberColumnStatistics";

        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + TABLE_NAME + " " +
                                "(constraintCollectionId, columnId, minValue, maxValue, average, stdDev) " +
                                "VALUES (?, ?, ?, ?, ?, ?);",
                        (parameters, preparedStatement) -> {
                            preparedStatement.setInt(1, (Integer) parameters[0]);
                            preparedStatement.setInt(2, (Integer) parameters[1]);
                            for (int sqlParameterIndex = 3; sqlParameterIndex < 7; sqlParameterIndex++) {
                                final double doubleValue = (double) parameters[sqlParameterIndex - 1];
                                if (Double.isNaN(doubleValue)) {
                                    preparedStatement.setNull(sqlParameterIndex, Types.REAL);
                                } else {
                                    preparedStatement.setDouble(sqlParameterIndex, doubleValue);
                                }
                            }
                        },
                        TABLE_NAME);

        private static final StrategyBasedPreparedQuery.Factory<Void> LOAD_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        String.format("SELECT columnId, minValue, maxValue, average, stdDev " +
                                "FROM %1$s;", TABLE_NAME),
                        PreparedStatementAdapter.VOID_ADAPTER,
                        TABLE_NAME);

        private static final StrategyBasedPreparedQuery.Factory<Integer> LOAD_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        String.format("SELECT columnId, minValue, maxValue, average, stdDev " +
                                "FROM %1$s " +
                                "WHERE constraintCollectionId = ?;", TABLE_NAME),
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        TABLE_NAME);

        private final SQLiteInterface sqLiteInterface;

        private final PreparedStatementBatchWriter<Object[]> insertWriter;

        private final DatabaseQuery<Void> loadQuery;

        private final DatabaseQuery<Integer> loadConstraintCollectionQuery;

        public SQLiteSerializer(SQLiteInterface sqLiteInterface) {
            this.sqLiteInterface = sqLiteInterface;
            try {
                final DatabaseAccess dba = this.sqLiteInterface.getDatabaseAccess();
                this.insertWriter = dba.createBatchWriter(INSERT_WRITER_FACTORY);
                this.loadQuery = dba.createQuery(LOAD_QUERY_FACTORY);
                this.loadConstraintCollectionQuery = dba.createQuery(LOAD_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getTableNames() {
            return Arrays.asList(TABLE_NAME);
        }

        @Override
        public void initializeTables() {
            if (!this.sqLiteInterface.tableExists(TABLE_NAME)) {
                String createTable = "CREATE TABLE [" + TABLE_NAME + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [constraintCollectionId] integer NOT NULL,\n" +
                        "    [columnId] integer NOT NULL,\n" +
                        "    [minValue] real,\n" +
                        "    [maxValue] real,\n" +
                        "    [average] real,\n" +
                        "    [stdDev] real,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "    FOREIGN KEY ([constraintCollectionId])\n" +
                        "    REFERENCES [ConstraintCollection] ([id]),\n" +
                        "    FOREIGN KEY ([columnId])\n" +
                        "    REFERENCES [Columnn] ([id])\n" +
                        ");";
                this.sqLiteInterface.executeCreateTableStatement(createTable);
            }
            if (!sqLiteInterface.tableExists(TABLE_NAME)) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void serialize(Constraint constraint, ConstraintCollection constraintCollection) {
            if (!(constraint instanceof NumberColumnStatistics)) {
                throw new IllegalArgumentException();
            }
            NumberColumnStatistics columnStatistics = (NumberColumnStatistics) constraint;
            try {
                this.insertWriter.write(new Object[]{
                        constraintCollection.getId(),
                        columnStatistics.getTargetReference().getTargetId(),
                        columnStatistics.getMaxValue(),
                        columnStatistics.getMaxValue(),
                        columnStatistics.getAverage(),
                        columnStatistics.getStandardDeviation()
                });
            } catch (SQLException e) {
                throw new RuntimeException("Serialization failed.", e);
            }
        }

        @Override
        public Collection<NumberColumnStatistics> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {

            Collection<NumberColumnStatistics> constraints = new LinkedList<>();
            try (ResultSet resultSet = constraintCollection == null ?
                    this.loadQuery.execute(null) :
                    this.loadConstraintCollectionQuery.execute(constraintCollection.getId())) {

                while (resultSet.next()) {
                    final NumberColumnStatistics constraint = new NumberColumnStatistics(resultSet.getInt("columnId"));
                    constraint.setMinValue(getNullableDouble(resultSet, "minValue", Double.NaN));
                    constraint.setMaxValue(getNullableDouble(resultSet, "maxValue", Double.NaN));
                    constraint.setAverage(getNullableDouble(resultSet, "average", Double.NaN));
                    constraint.setStandardDeviation(getNullableDouble(resultSet, "stdDev", Double.NaN));
                    constraints.add(constraint);
                }

            } catch (SQLException e) {
                throw new RuntimeException("Could not load constraint collection.", e);
            }
            return constraints;
        }



        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // todo
            throw new RuntimeException("Not implemented.");
        }
    }
}
