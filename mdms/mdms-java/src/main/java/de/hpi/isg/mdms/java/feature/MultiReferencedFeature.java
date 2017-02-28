package de.hpi.isg.mdms.java.feature;

import de.hpi.isg.mdms.java.util.Instance;
import de.hpi.isg.mdms.java.util.UnaryForeignKeyCandidate;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collection;

/**
 * Created by jianghm on 2016/10/18.
 */
public class MultiReferencedFeature extends Feature {

    private final static String MULTI_REFERENCED_FEATURE_NAME = "multi_referenced";

    /**
     * Stores the number of inds.
     */
    private int numINDs;

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        featureName = MULTI_REFERENCED_FEATURE_NAME;
        featureType = FeatureType.Numeric;

        numINDs = instanceCollection.size();

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
//            double normalized = normalize(numReferences);
            double normalized = numReferences;
            instance.getFeatureVector().put(MULTI_REFERENCED_FEATURE_NAME, normalized);
        }
    }

    @Override
    public double normalize(double valueForNormalizing) {
        return valueForNormalizing/(double)numINDs;
    }
}
