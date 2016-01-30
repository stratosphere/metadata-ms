package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier prefers foreign key constraints where the foreign key is not a referenced primary key at the same
 * time.
 */
public class DependentAndReferencedClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The maximum number of references to a foreign key attribute.
     */
    private final int maxReferences;

    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results
     */
    public DependentAndReferencedClassifier(double weight, int maxReferences) {
        super(weight);
        this.maxReferences = maxReferences;
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        // Count the number of references for the columns.
        Int2IntOpenHashMap columnNumReferences = new Int2IntOpenHashMap();
        columnNumReferences.defaultReturnValue(0);
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            final int refColumn = fkc.getReferencedColumnId();
            columnNumReferences.addTo(refColumn, 1);
        }

        // Do the actual classification.
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            final int numReferences = columnNumReferences.get(depColumn);
            if (numReferences > this.maxReferences) {
                classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }
        }

    }
}
