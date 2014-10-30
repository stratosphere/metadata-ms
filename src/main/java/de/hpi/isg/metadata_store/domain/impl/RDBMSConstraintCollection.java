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

public class RDBMSConstraintCollection extends AbstractIdentifiable implements ConstraintCollection {

    private static final long serialVersionUID = -2911473574180511468L;

    private Collection<Constraint> constraints;

    private Collection<Target> scope;

    @ExcludeHashCodeEquals
    private SQLInterface sqlInterface;

    public RDBMSConstraintCollection(int id, Set<Constraint> constraints, Set<Target> scope, SQLInterface sqlInterface) {
        super(id);
        this.constraints = constraints;
        this.scope = scope;
        this.sqlInterface = sqlInterface;
    }

    public RDBMSConstraintCollection(int id, SQLInterface sqlInterface) {
        super(id);
        this.sqlInterface = sqlInterface;
    }

    @Override
    public Collection<Constraint> getConstraints() {
        if (this.constraints != null) {
            return Collections.unmodifiableCollection(this.constraints);
        }
        return Collections.unmodifiableCollection(this.sqlInterface.getAllConstraintsOrOfConstraintCollection(this));
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

    public void setConstraints(Collection<Constraint> constraints) {
        this.constraints = constraints;
    }

    public void setScope(Collection<Target> scope) {
        this.scope = scope;
    }

    @Override
    public String toString() {
        return "RDBMSConstraintCollection [constraints=" + constraints + ", scope=" + scope + ", getId()=" + getId()
                + "]";
    }

    @Override
    public void add(Constraint constraint) {
        this.constraints.add(constraint);
        for (Target t : constraint.getTargetReference().getAllTargets()) {
            this.scope.add((Target) t);
        }
    }

}
