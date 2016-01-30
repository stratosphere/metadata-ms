package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.MetadataStore;
import org.simmetrics.metrics.LongestCommonSubstring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier assumes that the foreign key column and the referenced primary key column should have a similar
 * average value. Additionally, we incorporate the standard deviation in the value lengths.
 */
public class TableNameDiffClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The mininum length of common subsequence between two table names that indicates a foreign key relationship
     * between those tables.
     */
    private final int minCommonSubsequenceLength;

    /**
     * {@link MetadataStore} that contains tables etc.
     */
    private final MetadataStore metadataStore;

    /**
     * Metric used to compare table names.
     */
    private final LongestCommonSubstring distance;


    /**
     * Creates a new instance.
     */
    public TableNameDiffClassifier(double weight,
                                   int minCommonSubsequenceLength,
                                   MetadataStore metadataStore) {
        super(weight);
        this.minCommonSubsequenceLength = minCommonSubsequenceLength;
        this.metadataStore = metadataStore;
        this.distance = new LongestCommonSubstring();
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            String depTableName = getContainingTableName(fkc.getDependentColumnId());
            String refTableName = getContainingTableName(fkc.getReferencedColumnId());

            // Re-establish the hidden "longest common subsequence" function.
            int longestCommonSubsequence = (depTableName.length() + refTableName.length()
                    - (int) this.distance.distance(depTableName, refTableName)) / 2;

            if (longestCommonSubsequence >= this.minCommonSubsequenceLength) {
                classificationSet.addPartialResult(new WeightedResult(Result.FOREIGN_KEY));
            } else {
                classificationSet.addPartialResult(new WeightedResult(Result.UNKNOWN));
            }
        }

    }

    /**
     * Returns the name of a table that contains a certain column.
     *
     * @param columnId the ID of the column
     * @return the table name
     */
    private String getContainingTableName(int columnId) {
        final int tableId = this.metadataStore.getIdUtils().getTableId(columnId);
        final int schemaId = this.metadataStore.getIdUtils().getSchemaId(columnId);
        return this.metadataStore.getSchemaById(schemaId).getTableById(tableId).getName();
    }

}
