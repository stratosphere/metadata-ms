package de.hpi.isg.mdms.model.targets;

import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.common.AbstractIdentifiableAndNamed;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.common.Printable;

/**
 * {@link AbstractTarget} is a convenience class for all {@link Target} implementation already taking car of
 * {@link Location} and holding the observing {@link MetadataStore} that acts as an {@link Observer} where new targets
 * have to be registered right after creation.
 *
 */

public abstract class AbstractTarget extends AbstractIdentifiableAndNamed implements Target {

    private static final long serialVersionUID = -583488154227852034L;

    @ExcludeHashCodeEquals
    private final transient Observer observer;

    @Printable
    private final Location location;

    @Printable
    private String description;

    public AbstractTarget(final Observer observer, final int id, final String name, String description,
            final Location location) {
        super(observer, id, name);
        this.location = location;
        this.observer = observer;
        this.description = description != null ? description : "";
    }

    @Override
    public Location getLocation() {
        return this.location;
    }

    protected Observer getObserver() {
        return this.observer;
    }

    @Override
    public void register() {
        //observer.registerId(this.getId());
        this.observer.registerTargetObject(this);
    }

    /*
     * @Override public int hashCode() { return getId(); }
     * @Override public boolean equals(Object obj) { if (this == obj) return true; if (!super.equals(obj)) return false;
     * if (getClass() != obj.getClass()) return false; AbstractTarget other = (AbstractTarget) obj; return this.getId()
     * == other.getId(); }
     */

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }
}
