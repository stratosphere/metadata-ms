package de.hpi.isg.metadata_store.domain.common.impl;

import java.io.Serializable;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;
import de.hpi.isg.metadata_store.domain.common.Observer;

/**
 * This abstract class is a convenience class taking car for {@link Named} and {@link Identifiable} objects by providing
 * the fields and accesors.
 *
 */
public abstract class AbstractIdentifiableAndNamed extends AbstractHashCodeAndEquals implements Identifiable, Named,
        Serializable {

    private static final long serialVersionUID = -2489903063142674900L;

    @Printable
    private int id;
    @Printable
    private String name;

    public AbstractIdentifiableAndNamed(final Observer observer, int id, final String name) {
        if (id == -1) {
            id = observer.generateRandomId();
        }

        this.id = id;
        this.name = name;
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
