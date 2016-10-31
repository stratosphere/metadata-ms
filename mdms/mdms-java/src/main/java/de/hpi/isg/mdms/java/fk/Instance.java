package de.hpi.isg.mdms.java.fk;

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;

import java.util.Map;
import java.util.Objects;

/**
 * Represent an {@link Instance} in the {@link Dataset}.
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

    public Result getIsForeignKey() {
        return isForeignKey;
    }

    public void setIsForeignKey(Result isForeignKey) {
        this.isForeignKey = isForeignKey;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Instance instance = (Instance) o;
        return isForeignKey == instance.isForeignKey &&
                Objects.equals(featureVector, instance.featureVector) &&
                Objects.equals(fkCandidate, instance.fkCandidate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isForeignKey, featureVector, fkCandidate);
    }
}
