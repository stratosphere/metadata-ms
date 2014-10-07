package de.hpi.isg.metadata_store.domain.common.impl;

import java.io.Serializable;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Observer;

/**
 * This abstract class is a convenience class taking care for
 * {@link Identifiable} objects by providing the field and accesors.
 *
 */
public abstract class AbstractIdentifiable extends AbstractHashCodeAndEquals implements Identifiable, Serializable {

    private static final long serialVersionUID = -2489903063142674900L;
    private int id;

    public AbstractIdentifiable(Observer observer, int id) {
	if (id == -1) {
	    id = observer.generateRandomId();
	}
	observer.registerId(id);
	this.id = id;
    }

    @Override
    public int getId() {
	return this.id;
    }

    public void setId(int id) {
	this.id = id;
    }

}
