package de.hpi.isg.mdms.java.fk.classifiers;

import de.hpi.isg.mdms.java.fk.ClassificationSet;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.model.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This classifier assumes that the foreign key column and the referenced primary key column should have a similar
 * average value. Additionally, we incorporate the standard deviation in the value lengths.
 */
public class ShortReferencedTableNameClassifier extends PartialForeignKeyClassifier {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The mininum length of common subsequence between two table names that indicates a foreign key relationship
     * between those tables.
     */
    private final int maxReferencedTableNameLength;

    /**
     * {@link MetadataStore} that contains tables etc.
     */
    private final MetadataStore metadataStore;


    /**
     * Creates a new instance.
     */
    public ShortReferencedTableNameClassifier(double weight,
                                              int maxReferencedTableNameLength,
                                              MetadataStore metadataStore) {
        super(weight);
        this.maxReferencedTableNameLength = maxReferencedTableNameLength;
        this.metadataStore = metadataStore;
    }

    @Override
    public void classify(Collection<ClassificationSet> classificationSets) {
        for (ClassificationSet classificationSet : classificationSets) {
            final UnaryForeignKeyCandidate fkc = classificationSet.getForeignKeyCandidate();
            String refTableName = getContainingTableName(fkc.getReferencedColumnId());

            if (refTableName.length() >= this.maxReferencedTableNameLength) {
                classificationSet.addPartialResult(new WeightedResult(Result.NO_FOREIGN_KEY));
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
