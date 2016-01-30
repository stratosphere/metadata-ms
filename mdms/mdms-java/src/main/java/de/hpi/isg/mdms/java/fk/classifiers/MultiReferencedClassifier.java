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
public class MultiReferencedClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * A significant number of references to a primary key.
     */
    private final int significantRefCount;

    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results
     */
    public MultiReferencedClassifier(double weight, int significantRefCount) {
        super(weight);
        this.significantRefCount = significantRefCount;
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
            final int refColumn = fkc.getReferencedColumnId();
            final int numReferences = columnNumReferences.get(refColumn);
            if (numReferences >= this.significantRefCount) {
                classificationSet.addPartialResult(new WeightedResult(Result.FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }
        }

    }
}
