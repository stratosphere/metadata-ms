package de.hpi.isg.metadata_store.domain.impl;

import java.util.HashMap;
import java.util.Map;

import de.hpi.isg.metadata_store.domain.IConstraint;
import de.hpi.isg.metadata_store.domain.ITargetReference;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;

public class AbstractConstraint extends AbstractIdentifiableAndNamed implements IConstraint {

    private static final long serialVersionUID = 6125996484450631741L;

    private Map<Object, Object> properties;
    private ITargetReference target;

    public AbstractConstraint(long id, String name, ITargetReference target) {
	super(id, name);
	this.properties = new HashMap<Object, Object>();
	this.target = target;
    }

    @Override
    public Map<Object, Object> getProperties() {
	return properties;
    }

    @Override
    public ITargetReference getTargetReference() {
	return target;
    }

}
