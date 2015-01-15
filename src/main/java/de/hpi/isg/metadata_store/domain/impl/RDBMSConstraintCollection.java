package de.hpi.isg.metadata_store.domain.impl;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiable;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class RDBMSConstraintCollection extends AbstractIdentifiable implements ConstraintCollection {

    private static final long serialVersionUID = -2911473574180511468L;
    
    public static final boolean IS_CHECK_CONSTRAINT_TARGETS = false;

    private Collection<Constraint> constraints = null;

    private Collection<Target> scope;

    private String description;

    @ExcludeHashCodeEquals
    private SQLInterface sqlInterface;

    public RDBMSConstraintCollection(int id, String description, Set<Target> scope, SQLInterface sqlInterface) {
        super(id);
        this.scope = scope;
        this.sqlInterface = sqlInterface;
        this.description = description != null ? description : "";
    }

    public RDBMSConstraintCollection(int id, String description, SQLInterface sqlInterface) {
        super(id);
        this.sqlInterface = sqlInterface;
        this.description = description;
    }

    @Override
    public Collection<Constraint> getConstraints() {
        ensureConstraintsLoaded();
        return Collections.unmodifiableCollection(this.sqlInterface.getAllConstraintsForConstraintCollection(this));
    }

    private void ensureConstraintsLoaded() {
        if (this.constraints == null) {
            this.constraints = this.sqlInterface.getAllConstraintsForConstraintCollection(this);
        }
    }

    @Override
    public Collection<Target> getScope() {
        if (this.scope != null) {
            return Collections.unmodifiableCollection(this.scope);
        }
        return Collections.unmodifiableCollection(this.sqlInterface.getScopeOfConstraintCollection(this));
    }

    public SQLInterface getSqlInterface() {
        return sqlInterface;
    }
    
    @Override
    public MetadataStore getMetadataStore() {
        return this.sqlInterface.getMetadataStore();
    }

    public void setScope(Collection<Target> scope) {
        this.scope = scope;
    }

    @Override
    public String toString() {
        return "RDBMSConstraintCollection [scope=" + scope + ", getId()=" + getId() + "]";
    }

    @Override
    public void add(Constraint constraint) {
        this.constraints = null;

        if (IS_CHECK_CONSTRAINT_TARGETS) {
            // Ensure that all targets of the constraint are valid.
            for (IntIterator i = constraint.getTargetReference().getAllTargetIds().iterator(); i.hasNext();) {
                int targetId = i.nextInt();
                try {
                    if (!this.sqlInterface.isTargetIdInUse(targetId)) {
                        throw new NotAllTargetsInStoreException(targetId);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        // Write the constraint.
        this.sqlInterface.writeConstraint(constraint);
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    /*
     * @Override public boolean equals(Object obj) { ensureConstraintsLoaded(); if (obj instanceof
     * RDBMSConstraintCollection) { ((RDBMSConstraintCollection) obj).ensureConstraintsLoaded(); } return
     * super.equals(obj); }
     * @Override public int hashCode() { ensureConstraintsLoaded(); return super.hashCode(); }
     */

}
