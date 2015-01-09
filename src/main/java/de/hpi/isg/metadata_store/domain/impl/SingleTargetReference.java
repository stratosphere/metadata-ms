package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntLists;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

/**
 * A {@link TargetReference} with only one {@link Target}.
 *
 */
public class SingleTargetReference extends AbstractHashCodeAndEquals implements TargetReference {
    
    private static final long serialVersionUID = 9068771036941499754L;

    private final int targetId;

    /**
     * Creates a new instance pointing to the given target.
     * 
     * @param target is the target to reference
     * @deprecated Use {@link #SingleTargetReference(int)} instead to avoid providing the domain object
     */
    public SingleTargetReference(final Target target) {
        this.targetId = target.getId();
    }
    
    public SingleTargetReference(final int targetId) {
        this.targetId = targetId;
    }

    @Override
    public IntCollection getAllTargetIds() {
        return IntLists.singleton(this.targetId);
    }

    @Override
    public String toString() {
        return "SingleTargetReference[" + targetId + "]";
    }

    
    
}
