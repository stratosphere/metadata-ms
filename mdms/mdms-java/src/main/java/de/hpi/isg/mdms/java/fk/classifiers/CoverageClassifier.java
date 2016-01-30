package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.domain.constraints.DistinctValueCount;
import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier distinguishes foreign key constraints by the fact that the foreign key should have a similar
 * amount of distinct values compared to the referenced primary key.
 */
public class CoverageClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Stores the distinct value counts for all columns.
     */
    private final Int2LongMap distinctValues;

    /**
     * The minimum dep/ref distinct value ratio for the IND to be considered a foreign key.
     */
    private final double fkRatio;


    /**
     * The maximum dep/ref distinct value ratio for the IND to be considered not a foreign key.
     */
    private final double nonFkRatio;

    public CoverageClassifier(double weight,
                              double fkRatio, double nonFkRatio,
                              ConstraintCollection distinctValuesConstraintCollection) {
        super(weight);

        this.fkRatio = fkRatio;
        this.nonFkRatio = nonFkRatio;
        // Initialize the distinct value counts.
        this.distinctValues = new Int2LongOpenHashMap(distinctValuesConstraintCollection.getConstraints().size());
        distinctValuesConstraintCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof DistinctValueCount)
                .map(constraint -> (DistinctValueCount) constraint)
                .forEach(distinctValueCount -> distinctValues.put(
                        distinctValueCount.getTargetReference().getTargetId(),
                        distinctValueCount.getNumDistinctValues()));

    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();

            long depDistinctValueCount = this.distinctValues.get(fkc.getDependentColumnId());
            long refDistinctValueCount = this.distinctValues.get(fkc.getReferencedColumnId());

            if (depDistinctValueCount >= this.fkRatio * refDistinctValueCount) {
                classificationSet.addPartialResult(new WeightedResult(Result.FOREIGN_KEY));
            } else if (depDistinctValueCount <= this.nonFkRatio * refDistinctValueCount) {
                classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }

        }
    }
}
