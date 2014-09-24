package de.hpi.isg.metadata_store.domain.common.impl;

import java.io.Serializable;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;

public class AbstractIdentifiableAndNamed extends AbstractHashCodeAndEquals implements Identifiable, Named,
	Serializable {

    private static final long serialVersionUID = -2489903063142674900L;
    private long id;
    private String name;

    public AbstractIdentifiableAndNamed(long id, String name) {
	this.id = id;
	this.name = name;
    }

    @Override
    public long getId() {
	return id;
    }

    @Override
    public String getName() {
	return name;
    }

    public void setId(long id) {
	this.id = id;
    }

    public void setName(String name) {
	this.name = name;
    }

}
