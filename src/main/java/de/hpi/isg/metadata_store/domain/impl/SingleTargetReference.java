package de.hpi.isg.metadata_store.domain.impl;

import java.util.Arrays;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.ITarget;
import de.hpi.isg.metadata_store.domain.ITargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

public class SingleTargetReference extends AbstractHashCodeAndEquals implements ITargetReference {
    private static final long serialVersionUID = 9068771036941499754L;

    private final ITarget target;

    public SingleTargetReference(ITarget target) {
	this.target = target;
    }

    public ITarget getTarget() {
	return target;
    }

    @Override
    public Collection<ITarget> getAllTargets() {
	return Arrays.asList(target);
    }

}
