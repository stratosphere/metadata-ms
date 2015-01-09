package de.hpi.isg.metadata_store.exceptions;

import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSConstraintCollection;

public class ConstraintCollectionEmptyException extends RuntimeException {

    private static final long serialVersionUID = -6251921922755824708L;

    public ConstraintCollectionEmptyException(RDBMSConstraintCollection rdbmsConstraintCollection) {
        super("The queried constraint collection " + rdbmsConstraintCollection
                + " was empty. Maybe you forgot to register constraint type's serializer in the "
                + SQLiteInterface.class + "?");
    }
}
