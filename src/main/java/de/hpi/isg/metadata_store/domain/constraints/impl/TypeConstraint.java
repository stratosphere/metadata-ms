package de.hpi.isg.metadata_store.domain.constraints.impl;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.impl.AbstractConstraint;

public class TypeConstraint extends AbstractConstraint implements Constraint {

    private static final long serialVersionUID = 3194245498846860560L;

    public TypeConstraint(int id, String name, TargetReference target) {
	super(id, name, target);
    }

    @Override
    public String toString() {
	return "TypeConstraint [getProperties()=" + this.getProperties() + ", getTargetReference()="
		+ this.getTargetReference() + ", getId()=" + this.getId() + ", getName()=" + this.getName() + "]";
    }

}
