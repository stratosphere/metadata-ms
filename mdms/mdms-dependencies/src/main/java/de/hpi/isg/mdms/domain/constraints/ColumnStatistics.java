package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

import static de.hpi.isg.mdms.domain.util.SQLiteConstraintUtils.getNullableDouble;
import static de.hpi.isg.mdms.domain.util.SQLiteConstraintUtils.getNullableInt;

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
