package de.hpi.isg.metadata_store.domain.impl;

import java.util.HashMap;
import java.util.Map;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;

/**
 * {@link AbstractConstraint} is an abstract super class for all
 * {@link Constraint} implementations, already taking care of
 * {@link TargetReference} and the storing of the internal {@link Map}.
 *
 */

public class AbstractConstraint extends AbstractIdentifiableAndNamed implements Constraint {

    private static final long serialVersionUID = 6125996484450631741L;

    private final Map<Object, Object> properties;
    private final TargetReference target;

    public AbstractConstraint(int id, String name, TargetReference target) {
	super(id, name);
	this.properties = new HashMap<Object, Object>();
	this.target = target;
    }

    @Override
    public Map<Object, Object> getProperties() {
	return this.properties;
    }

    @Override
    public TargetReference getTargetReference() {
	return this.target;
    }

}
