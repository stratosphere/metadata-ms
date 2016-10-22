package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class MultiReferencedFeature extends Feature {

    private final static String MULTI_REFERENCED_FEATURE_NAME = "MultiReferenced";

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        featureName = MULTI_REFERENCED_FEATURE_NAME;

        // Count the number of references for the columns.
        Int2IntOpenHashMap columnNumReferences = new Int2IntOpenHashMap();
        columnNumReferences.defaultReturnValue(0);
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int refColumn = fkc.getReferencedColumnId();
            columnNumReferences.addTo(refColumn, 1);
        }

        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int refColumn = fkc.getReferencedColumnId();
            final int numReferences = columnNumReferences.get(refColumn);
            instance.getFeatureVector().put(MULTI_REFERENCED_FEATURE_NAME, numReferences);
        }
    }
}
