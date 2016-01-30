package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier prefers foreign key constraints where the foreign key is not a referenced primary key at the same
 * time.
 */
public class NoReferencingPKClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * IDs of the primary key columns.
     */
    private final IntSet pkColumnIds;

    /**
     * Creates a new instance.
     *
     * @param weight weight of the classifiers results
     */
    public NoReferencingPKClassifier(double weight, ConstraintCollection pkConstraintCollection) {
        super(weight);
        this.pkColumnIds = new IntOpenHashSet();
        pkConstraintCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof UniqueColumnCombination)
                .map(constraint -> (UniqueColumnCombination) constraint)
                .flatMap(ucc -> ucc.getTargetReference().getAllTargetIds().stream())
                .forEach(this.pkColumnIds::add);
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        // Do the actual classification.
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            if (this.pkColumnIds.contains(fkc.getDependentColumnId())) {
                classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }
        }

    }
}
