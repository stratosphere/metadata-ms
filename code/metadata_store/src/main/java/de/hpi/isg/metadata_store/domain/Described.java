package de.hpi.isg.metadata_store.domain;

/**
 * This interface provided the getter and setter for descriptions. Descriptions provide additional information about
 * objects in the {@link MetadataStore}. For example {@link Target}s an {@link ConstraintCollection} can have a
 * description.
 * 
 * @author fabian
 *
 */

public interface Described {

    String getDescription();

    void setDescription(String description);

}
