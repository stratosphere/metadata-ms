package de.hpi.isg.mdms.java.fk;

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;

import java.util.Map;

/**
 * Represent an instance in the dataset.
 * @author Lan Jiang
 */
public class Instance {

    /**
     * Used to decide whether a IND feature vector is a foreign key.
     */
    private Result isForeignKey;

    /**
     * Represents the feature vector of a instance.
     */
    private Map<String, Object> featureVector;

    /**
     * Represents the PK-FK pair of this instance.
     */
    private UnaryForeignKeyCandidate fkCandidate;

    public Instance(UnaryForeignKeyCandidate fkCandidate) {
        this.isForeignKey = Result.UNKNOWN;
        this.featureVector = new Object2ObjectArrayMap<>();
        this.fkCandidate = fkCandidate;
    }

    public Map<String, Object> getFeatureVector() {
        return featureVector;
    }

    public UnaryForeignKeyCandidate getForeignKeyCandidate() {
        return fkCandidate;
    }

    /**
     * Possible results of a partial foreign key classifier.
     */
    public enum Result {
        /**
         * Indicates that the classifier believes that an IND is a foreign key.
         */
        FOREIGN_KEY,

        /**
         * Indicates that the classifier is not sure whether an IND is a foreign key.
         */
        UNKNOWN,

        /**
         * Indicates that the classifier believes that an IND is not a foreign key.
         */
        NO_FOREIGN_KEY;
    }
}
