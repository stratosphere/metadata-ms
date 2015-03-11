package de.hpi.isg.mdms.model.common;

import java.io.Serializable;

/**
 * This abstract class is a convenience class taking car for {@link de.hpi.isg.mdms.model.common.Named} and {@link Identifiable} objects by providing
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
