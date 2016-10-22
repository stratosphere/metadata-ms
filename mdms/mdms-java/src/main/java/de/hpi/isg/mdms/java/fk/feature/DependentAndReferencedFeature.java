package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class DependentAndReferencedFeature extends Feature {

    private final static String DEPENDENT_AND_REFERENCED_FEATURE_NAME = "DependentAndReferenced";

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        featureName = DEPENDENT_AND_REFERENCED_FEATURE_NAME;

        // Count the number of references for the columns.
        Int2IntOpenHashMap columnNumReferences = new Int2IntOpenHashMap();
        columnNumReferences.defaultReturnValue(0);
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int refColumn = fkc.getReferencedColumnId();
            columnNumReferences.addTo(refColumn, 1);
        }

        // Do the actual classification.
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            final int numReferences = columnNumReferences.get(depColumn);
            instance.getFeatureVector().put(DEPENDENT_AND_REFERENCED_FEATURE_NAME, numReferences);
        }
    }
}
