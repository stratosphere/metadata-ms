package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.domain.constraints.DistinctValueCount;
import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier rejects foreign key constraints where the foreign key has too few distinct values.
 */
public class DistinctDependentValuesClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Stores the distinct value counts for all columns.
     */
    private final Int2LongMap distinctValues;

    /**
     * The minimum number of distinct values for a foreign key.
     */
    private final double minDistinctValues;

    public DistinctDependentValuesClassifier(double weight,
                                             int minDistinctValues,
                                             ConstraintCollection<? extends Constraint> distinctValuesConstraintCollection) {
        super(weight);
        this.minDistinctValues = minDistinctValues;

        // Initialize the distinct value counts.
        this.distinctValues = new Int2LongOpenHashMap(distinctValuesConstraintCollection.getConstraints().size());
        distinctValuesConstraintCollection.getConstraints().stream()
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

            if (depDistinctValueCount < this.minDistinctValues) {
                classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }

        }
    }
}
