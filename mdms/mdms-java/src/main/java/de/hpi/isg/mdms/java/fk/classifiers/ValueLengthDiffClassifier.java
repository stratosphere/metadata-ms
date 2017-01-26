package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.domain.constraints.TextColumnStatistics;
import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

import java.util.Collection;

/**
 * This classifier assumes that the foreign key column and the referenced primary key column should have a similar
 * average value length. Additionally, we incorporate the standard deviation in the value lengths.
 */
public class ValueLengthDiffClassifier extends PartialForeignKeyClassifier {

    /**
     * The average length of values by column.
     */
    private final Int2DoubleMap columnAvgValueLengths;

    /**
     * The standard deviation of value lengths by column.
     */
    private final Int2DoubleMap columnValueLengthStdDevs;

    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results
     */
    protected ValueLengthDiffClassifier(double weight,
                                        ConstraintCollection<? extends Constraint> textColumnStatisticsCollection,
                                        double maxLengthDelta,
                                        double maxStdDevDelta) {
        super(weight);

        this.columnAvgValueLengths = new Int2DoubleOpenHashMap();
        this.columnValueLengthStdDevs = new Int2DoubleOpenHashMap();

        for (Constraint constraint : textColumnStatisticsCollection.getConstraints()) {
            TextColumnStatistics textColumnStatistics = (TextColumnStatistics) constraint;
            throw new RuntimeException("Cannot run classifier: average string length not available yet.");
        }
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        throw new RuntimeException("Not implemented.");
    }
}
