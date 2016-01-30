package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier prefers foreign key constraints where the foreign key is the dependent column of only very few
 * inclusion dependencies.
 */
public class MultiDependentClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The maximum number of occurrences as dependent attribute in inclusion dependencies for a foreign key attribute.
     */
    private final int maxDependentOccurrences;

    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results
     */
    public MultiDependentClassifier(double weight, int maxDependentOccurrences) {
        super(weight);
        this.maxDependentOccurrences = maxDependentOccurrences;
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        // Count the number of references for the columns.
        Int2IntOpenHashMap columnNumDependentOccurrences = new Int2IntOpenHashMap();
        columnNumDependentOccurrences.defaultReturnValue(0);
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            columnNumDependentOccurrences.addTo(depColumn, 1);
        }

        // Do the actual classification.
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            final int numDependentOccurrences = columnNumDependentOccurrences.get(depColumn);
            if (numDependentOccurrences > this.maxDependentOccurrences) {
                classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }
        }

    }
}
