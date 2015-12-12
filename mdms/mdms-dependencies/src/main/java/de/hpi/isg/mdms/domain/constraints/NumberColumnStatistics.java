package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
            try {
                return new SQLiteSerializer((SQLiteInterface) sqlInterface);
            } catch (SQLException e) {
                throw new RuntimeException("Could not create serializer.", e);
            }
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
    private static class SQLiteSerializer implements ConstraintSQLSerializer<NumberColumnStatistics> {

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

        private final SQLiteInterface sqLiteInterface;

        private final PreparedStatementBatchWriter<Object[]> insertWriter;

        private SQLiteSerializer(SQLiteInterface sqLiteInterface) throws SQLException {
            this.sqLiteInterface = sqLiteInterface;
            this.insertWriter = this.sqLiteInterface.getDatabaseAccess().createBatchWriter(INSERT_WRITER_FACTORY);
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
                this.insertWriter.write(new Object[] {
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
        public Collection<NumberColumnStatistics> deserializeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // todo
            throw new RuntimeException("Not implemented.");
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // todo
            throw new RuntimeException("Not implemented.");
        }
    }
}
