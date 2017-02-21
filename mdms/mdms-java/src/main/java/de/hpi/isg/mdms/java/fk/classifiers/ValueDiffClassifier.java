package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.domain.constraints.NumberColumnStatistics;
import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier assumes that the foreign key column and the referenced primary key column should have a similar
 * average value. Additionally, we incorporate the standard deviation in the value lengths.
 */
public class ValueDiffClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The average values by column.
     */
    private final Int2DoubleMap columnAvgValue;

    /**
     * The standard deviation of values by column.
     */
    private final Int2DoubleMap columnValueStdDevs;

    /**
     * The minimum overlap of the ranges {@code (avg - stdDev, avg + stdDev)} of the dependent and referenced column,
     * normalized over the range spanned by both columns together..
     */
    private final double minOverlapRatio;

    /**
     * The maximum overlap of the ranges {@code (avg - stdDev, avg + stdDev)} of the dependent and referenced column,
     * normalized over the range spanned by both columns together before being classified as non-key.
     */
    private final double disqualifyingOverlapRatio;


    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results
     */
    public ValueDiffClassifier(double weight,
                               ConstraintCollection<? extends Constraint> textColumnStatisticsCollection,
                               double minOverlapRatio,
                               double disqualifyingOverlapRatio) {
        super(weight);
        this.minOverlapRatio = minOverlapRatio;
        this.disqualifyingOverlapRatio = disqualifyingOverlapRatio;

        // Index the required statistics.
        this.columnAvgValue = new Int2DoubleOpenHashMap();
        this.columnAvgValue.defaultReturnValue(Double.NaN);
        this.columnValueStdDevs = new Int2DoubleOpenHashMap();
        this.columnValueStdDevs.defaultReturnValue(Double.NaN);
        for (Constraint constraint : textColumnStatisticsCollection.getConstraints()) {
            if (!(constraint instanceof NumberColumnStatistics)) continue;
            NumberColumnStatistics numberColumnStatistics = (NumberColumnStatistics) constraint;
            final int columnId = numberColumnStatistics.getColumnId();
            this.columnAvgValue.put(columnId, numberColumnStatistics.getAverage());
            this.columnValueStdDevs.put(columnId, numberColumnStatistics.getStandardDeviation());
        }
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            final int depColumnId = fkc.getDependentColumnId();
            final int refColumnId = fkc.getReferencedColumnId();

            final double depColumnAvg = this.columnAvgValue.get(depColumnId);
            final double refColumnAvg = this.columnAvgValue.get(refColumnId);
            final double depColumnStdDev = this.columnValueStdDevs.get(depColumnId);
            final double refColumnStdDev = this.columnValueStdDevs.get(refColumnId);

            if (Double.isNaN(depColumnAvg) || Double.isNaN(refColumnAvg) || Double.isNaN(this.minOverlapRatio)
                    || Double.isNaN(depColumnStdDev) || Double.isNaN(refColumnStdDev)) {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            } else {
                double depBottom = depColumnAvg - depColumnStdDev;
                double depTop = depColumnAvg + depColumnStdDev;
                double refBottom = refColumnAvg - refColumnStdDev;
                double refTop = refColumnAvg + refColumnStdDev;
                double totalRange = Math.max(refTop, depTop) - Math.min(refBottom, depBottom);
                double overlap = Math.max(0, Math.min(depTop, refTop) - Math.max(depBottom, refBottom));
                if (totalRange * this.minOverlapRatio <= overlap) {
                    classificationSet.addPartialResult(new WeightedResult(Result.FOREIGN_KEY));
                } else if (totalRange * this.disqualifyingOverlapRatio > overlap) {
                    classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
                } else {
                    classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
                }
            }
        }
    }
}
