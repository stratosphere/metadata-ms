package de.hpi.isg.metadata_store.exceptions;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;

/**
 * This {@link Exception} is thrown if the user tries to add a {@link Constraint} to a {@link MetadataStore} with referenced {@link Target} (via {@link TargetReference}) that are unknown to the {@link MetadataStore}.
 * @author fabian
 *
 */
public class NotAllTargetsInStoreException extends IllegalStateException {

    private static final long serialVersionUID = 7552244497128771206L;

    public NotAllTargetsInStoreException(Target target) {
	super(target.toString());
    }
}
