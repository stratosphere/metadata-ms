package de.hpi.isg.metadata_store.domain.constraints.impl;

import de.hpi.isg.metadata_store.domain.IConstraint;
import de.hpi.isg.metadata_store.domain.ITargetReference;
import de.hpi.isg.metadata_store.domain.impl.AbstractConstraint;

public class TypeConstraint extends AbstractConstraint implements IConstraint {

    private static final long serialVersionUID = 3194245498846860560L;

    public TypeConstraint(long id, String name, ITargetReference target) {
	super(id, name, target);
    }

    @Override
    public String toString() {
	return "TypeConstraint [getProperties()=" + getProperties() + ", getTargetReference()=" + getTargetReference()
		+ ", getId()=" + getId() + ", getName()=" + getName() + "]";
    }

}
