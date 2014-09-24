package de.hpi.isg.metadata_store.domain.impl;

import java.util.Arrays;
import java.util.Collection;

import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;

public class SingleTargetReference extends AbstractHashCodeAndEquals implements TargetReference {
    private static final long serialVersionUID = 9068771036941499754L;

    private final Target target;

    public SingleTargetReference(Target target) {
	this.target = target;
    }

    public Target getTarget() {
	return target;
    }

    @Override
    public Collection<Target> getAllTargets() {
	return Arrays.asList(target);
    }

}
