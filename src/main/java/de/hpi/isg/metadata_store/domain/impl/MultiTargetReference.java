package de.hpi.isg.metadata_store.domain.impl;

import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

/**
 * A {@link TargetReference} with multiple {@link Target}s.
 *
 */
public class MultiTargetReference extends AbstractHashCodeAndEquals implements TargetReference {

    private static final long serialVersionUID = -4736784950630736653L;

    @Override
    public Collection<Target> getAllTargets() {
        // TODO Auto-generated method stub
        return null;
    }

}
