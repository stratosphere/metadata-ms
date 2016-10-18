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
public class MultiDependentFeature extends Feature{

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static String MULTI_DEPENDENT_FEATURE_NAME = "MultiDependent";

    @Override
    public void calcualteFeatureValue(Collection<Instance> instanceCollection) {
        // Count the number of references for the columns.
        Int2IntOpenHashMap columnNumDependentOccurrences = new Int2IntOpenHashMap();
        columnNumDependentOccurrences.defaultReturnValue(0);
        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            columnNumDependentOccurrences.addTo(depColumn, 1);
        }

        for (Instance instance : instanceCollection) {
            final UnaryForeignKeyCandidate fkc = instance.getForeignKeyCandidate();
            final int depColumn = fkc.getDependentColumnId();
            final int numDependentOccurrences = columnNumDependentOccurrences.get(depColumn);
            instance.getFeatureVector().put(MULTI_DEPENDENT_FEATURE_NAME, numDependentOccurrences);
        }

    }
}
