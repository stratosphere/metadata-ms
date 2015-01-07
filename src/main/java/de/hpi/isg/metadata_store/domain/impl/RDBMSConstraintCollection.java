package de.hpi.isg.metadata_store.domain.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiable;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class RDBMSConstraintCollection extends AbstractIdentifiable implements ConstraintCollection {

    private static final long serialVersionUID = -2911473574180511468L;

    private Collection<Constraint> constraints = null;

    private Collection<Target> scope;

    @ExcludeHashCodeEquals
    private SQLInterface sqlInterface;

    public RDBMSConstraintCollection(int id, Set<Target> scope, SQLInterface sqlInterface) {
        super(id);
        this.scope = scope;
        this.sqlInterface = sqlInterface;
    }

    public RDBMSConstraintCollection(int id, SQLInterface sqlInterface) {
        super(id);
        this.sqlInterface = sqlInterface;
    }

    @Override
    public Collection<Constraint> getConstraints() {
        ensureConstraintsLoaded();
        return Collections.unmodifiableCollection(this.sqlInterface.getAllConstraintsOrOfConstraintCollection(this));
    }

    private void ensureConstraintsLoaded() {
        if (this.constraints == null) {
            this.constraints = this.sqlInterface.getAllConstraintsOrOfConstraintCollection(this);
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
        
        // Ensure that all targets of the constraint are valid.
        for (final Target target : constraint.getTargetReference().getAllTargets()) {
            if (!this.sqlInterface.getAllTargets().contains(target)) {
                throw new NotAllTargetsInStoreException(target);
            }

        }

        // Write the constraint.
        this.sqlInterface.writeConstraint(constraint);
    }
    
    @Override
    public boolean equals(Object obj) {
        ensureConstraintsLoaded();
        if (obj instanceof RDBMSConstraintCollection) {
            ((RDBMSConstraintCollection) obj).ensureConstraintsLoaded();
        }
        return super.equals(obj);
    }
    
    @Override
    public int hashCode() {
        ensureConstraintsLoaded();
        return super.hashCode();
    }

}
