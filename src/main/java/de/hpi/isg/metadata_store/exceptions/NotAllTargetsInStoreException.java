package de.hpi.isg.metadata_store.exceptions;

import de.hpi.isg.metadata_store.domain.Target;

public class NotAllTargetsInStoreException extends IllegalStateException {

    private static final long serialVersionUID = 7552244497128771206L;

    public NotAllTargetsInStoreException(Target target) {
	super(target.toString());
    }
}
