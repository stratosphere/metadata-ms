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

    public SingleTargetReference(final int targetId) {
        this.targetId = targetId;
    }

    public int getTargetId() {
        return targetId;
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
