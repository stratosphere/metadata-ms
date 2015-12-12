package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This constraint class encapsulates various general single column statistics.
 */
public class ColumnStatistics implements RDBMSConstraint {

    private long numNulls = -1, numDistinctValues = -1;

    private double fillStatus = Double.NaN, uniqueness = Double.NaN;

    public List<ValueOccurrence> topKFrequentValues;

    private final SingleTargetReference targetReference;

    public ColumnStatistics(int columnId) {
        this.targetReference = new SingleTargetReference(columnId);
    }


    @Override
    public SingleTargetReference getTargetReference() {
        return this.targetReference;
    }

    @Override
    public ConstraintSQLSerializer<ColumnStatistics> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new SQLiteSerializer((SQLiteInterface) sqlInterface);
        }
        throw new RuntimeException("No serializer available for " + sqlInterface);
    }

    public long getNumNulls() {
        return numNulls;
    }

    public void setNumNulls(long numNulls) {
        this.numNulls = numNulls;
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public void setNumDistinctValues(long numDistinctValues) {
        this.numDistinctValues = numDistinctValues;
    }

    public double getFillStatus() {
        return fillStatus;
    }

    public void setFillStatus(double fillStatus) {
        this.fillStatus = fillStatus;
    }

    public double getUniqueness() {
        return uniqueness;
    }

    public void setUniqueness(double uniqueness) {
        this.uniqueness = uniqueness;
    }

    public List<ValueOccurrence> getTopKFrequentValues() {
        return topKFrequentValues;
    }

    public void setTopKFrequentValues(List<ValueOccurrence> topKFrequentValues) {
        this.topKFrequentValues = topKFrequentValues;
    }

    /**
     * SQLite serializer for {@link ColumnStatistics}.
     */
    public static class SQLiteSerializer implements ConstraintSQLSerializer<ColumnStatistics> {

        /**
         * Name of the SQL table to store the {@link ColumnStatistics} instances.
         */
        private static final String TABLE_NAME = "columnStatistics";

        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + TABLE_NAME + " (constraintCollectionId, columnId, nulls, fillStatus, distinctValues, uniqueness, topKValues) VALUES (?, ?, ?, ?, ?, ?, ?);",
                        (parameters, preparedStatement) -> {
                            preparedStatement.setInt(1, (Integer) parameters[0]);
                            preparedStatement.setInt(2, (Integer) parameters[1]);
                            for (int sqlParamIndex : Arrays.asList(3, 5)) {
                                long value = (long) (parameters[sqlParamIndex - 1]);
                                if (value == -1) {
                                    preparedStatement.setNull(sqlParamIndex, Types.INTEGER);
                                } else {
                                    preparedStatement.setLong(sqlParamIndex, value);
                                }
                            }
                            for (int sqlParamIndex : Arrays.asList(4, 6)) {
                                double value = (double) (parameters[sqlParamIndex - 1]);
                                if (Double.isNaN(value)) {
                                    preparedStatement.setNull(sqlParamIndex, Types.REAL);
                                } else {
                                    preparedStatement.setDouble(sqlParamIndex, value);
                                }
                            }
                            for (int sqlParamIndex : Arrays.asList(7)) {
                                List<ValueOccurrence> value = (List<ValueOccurrence>) (parameters[sqlParamIndex - 1]);
                                if (value == null) {
                                    preparedStatement.setNull(sqlParamIndex, Types.VARCHAR);
                                } else {
                                    preparedStatement.setString(sqlParamIndex, toJSONString(value));
                                }
                            }
                        },
                        TABLE_NAME);

        private final SQLiteInterface sqLiteInterface;

        private final PreparedStatementBatchWriter<Object[]> insertWriter;

        public SQLiteSerializer(SQLiteInterface sqLiteInterface) {
            this.sqLiteInterface = sqLiteInterface;
            try {
                this.insertWriter = this.sqLiteInterface.getDatabaseAccess().createBatchWriter(INSERT_WRITER_FACTORY);
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
                        "    [nulls] integer,\n" +
                        "    [fillStatus] real,\n" +
                        "    [distinctValues] integer,\n" +
                        "    [uniqueness] real,\n" +
                        "    [topKValues] text,\n" +
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
            if (!(constraint instanceof ColumnStatistics)) {
                throw new IllegalArgumentException();
            }
            ColumnStatistics columnStatistics = (ColumnStatistics) constraint;
            try {
                this.insertWriter.write(new Object[]{
                        constraintCollection.getId(),
                        columnStatistics.getTargetReference().getTargetId(),
                        columnStatistics.numNulls,
                        columnStatistics.fillStatus,
                        columnStatistics.numDistinctValues,
                        columnStatistics.uniqueness,
                        columnStatistics.topKFrequentValues
                });
            } catch (SQLException e) {
                throw new RuntimeException("Serialization failed.", e);
            }
        }

        @Override
        public Collection<ColumnStatistics> deserializeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // todo
            throw new RuntimeException("Not implemented.");
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // todo
            throw new RuntimeException("Not implemented.");
        }

        /**
         * Creates a (descendingly sorted) JSON array string from the top k frequent values. Each value is represented
         * by a JSON object with members {@code value} and {@code count}.
         *
         * @param topKFrequentValues the top k frequent values
         * @return the JSON array string
         */
        private static String toJSONString(List<ValueOccurrence> topKFrequentValues) {
            final List<JSONObject> topKEntryList = topKFrequentValues.stream()
                    .sorted((occ1, occ2) -> occ2.compareTo(occ1))
                    .map((occ) -> {
                        JSONObject topKEntry = new JSONObject();
                        topKEntry.put("value", occ.getValue());
                        topKEntry.put("count", occ.getNumOccurrences());
                        return topKEntry;
                    })
                    .collect(Collectors.toList());
            final String topKJsonString = new JSONArray(topKEntryList).toString();
            return topKJsonString;
        }
    }

    /**
     * This class describes a value and the number of its occurrences (in a column). Instances are primarily ordered by
     * their count and by their value as tie breaker.
     */
    public static class ValueOccurrence implements Comparable<ValueOccurrence> {

        private final String value;

        private final long numOccurrences;

        public ValueOccurrence(String value, long numOccurrences) {
            this.value = value;
            this.numOccurrences = numOccurrences;
        }

        public String getValue() {
            return value;
        }

        public long getNumOccurrences() {
            return numOccurrences;
        }

        @Override
        public int compareTo(ValueOccurrence that) {
            int result = Long.compare(this.getNumOccurrences(), that.getNumOccurrences());
            return result == 0 ? this.getValue().compareTo(that.getValue()) : result;
        }
    }
}
