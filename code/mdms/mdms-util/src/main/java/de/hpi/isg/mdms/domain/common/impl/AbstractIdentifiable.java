package de.hpi.isg.mdms.domain.common.impl;

import java.io.Serializable;

import de.hpi.isg.mdms.domain.common.Identifiable;
import de.hpi.isg.mdms.domain.common.Observer;

/**
 * This abstract class is a convenience class taking care for {@link Identifiable} objects by providing the field and
 * accesors.
 *
 */
public abstract class AbstractIdentifiable extends AbstractHashCodeAndEquals implements Identifiable, Serializable {

    private static final long serialVersionUID = -2489903063142674900L;
    private int id;

    public AbstractIdentifiable(final Observer observer, int id) {
        if (id == -1) {
            id = observer.generateRandomId();
        }
        this.id = id;
    }

    public AbstractIdentifiable(int id) {
        this.id = id;
    }

    @Override
    public int getId() {
        return this.id;
    }

    public void setId(final int id) {
        this.id = id;
    }

}
