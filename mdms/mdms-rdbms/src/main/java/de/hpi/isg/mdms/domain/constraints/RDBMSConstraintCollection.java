package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.IntIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The default implementation of a {@link ConstraintCollection} that is used in {@link de.hpi.isg.mdms.domain.RDBMSMetadataStore}s.
 *
 * @author fabian
 */

public class RDBMSConstraintCollection<T extends Constraint> extends AbstractIdentifiable implements ConstraintCollection<T> {

    private static final long serialVersionUID = -2911473574180511468L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSConstraintCollection.class);

    public static final boolean IS_CHECK_CONSTRAINT_TARGETS = false;

    private Collection<T> constraints = null;

    private Collection<Target> scope;

    private Set<Integer> scopeIdSet;

    private String description;

    private Experiment experiment = null;

    @ExcludeHashCodeEquals
    private Class<T> constrainttype;

    @ExcludeHashCodeEquals
    private SQLInterface sqlInterface;

    public RDBMSConstraintCollection(int id, String description, Set<Target> scope, SQLInterface sqlInterface, Class<T> constrainttype) {
        super(id);
        this.scope = scope;
        this.scopeIdSet = rebuildScopeSet(scope);
        this.sqlInterface = sqlInterface;
        this.description = description != null ? description : "";
        this.constrainttype = constrainttype;
    }

    public RDBMSConstraintCollection(int id, String description, Experiment experiment, Set<Target> scope, SQLInterface sqlInterface, Class<T> constrainttype) {
        super(id);
        this.scope = scope;
        this.scopeIdSet = rebuildScopeSet(scope);
        this.sqlInterface = sqlInterface;
        this.description = description != null ? description : "";
        this.experiment = experiment;
        this.constrainttype = constrainttype;
    }


    private Set<Integer> rebuildScopeSet(Collection<Target> scope) {
        Set<Integer> set = new HashSet<>();
        for (Target t : scope) {
            set.add(t.getId());
        }
        return set;
    }

    public RDBMSConstraintCollection(int id, String description, SQLInterface sqlInterface) {
        super(id);
        this.sqlInterface = sqlInterface;
        this.description = description;
    }

    public RDBMSConstraintCollection(int id, String description, Experiment experiment, SQLInterface sqlInterface) {
        super(id);
        this.sqlInterface = sqlInterface;
        this.description = description;
        this.experiment = experiment;
    }


    @Override
    public Collection<T> getConstraints() {
        ensureConstraintsLoaded();
        return constraints;
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

    public void setScope(Set<Target> scope) {
        // We enforce the Set type to support equals properly.
        this.scope = scope;
        this.scopeIdSet = rebuildScopeSet(scope);
    }

    @Override
    public String toString() {
        return "RDBMSConstraintCollection [scope=" + scope + ", getId()=" + getId() + "]";
    }

    @Override
    public void add(T constraint) {
        this.constraints = null;

        if (IS_CHECK_CONSTRAINT_TARGETS) {
            // Ensure that all targets of the constraint are valid.
            for (IntIterator i = constraint.getTargetReference().getAllTargetIds().iterator(); i.hasNext(); ) {
                int targetId = i.nextInt();
                if (!targetInScope(targetId)) {
                    LOGGER.warn("Target with id {} not in scope of constraint collection", targetId);
                }
            }

        }

        // Write the constraint.
        this.sqlInterface.writeConstraint(constraint, this);
    }

    private boolean targetInScope(int targetId) {
        IdUtils idUtils = this.sqlInterface.getMetadataStore().getIdUtils();

        if (this.scopeIdSet.contains(targetId)) {
            return true;
        }

        switch (idUtils.getIdType(targetId)) {
            case SCHEMA_ID:
                return false;
            case TABLE_ID:
                return this.scopeIdSet.contains(idUtils.getSchemaId(targetId));
            case COLUMN_ID:
                return this.scopeIdSet.contains(idUtils.getSchemaId(targetId))
                        || this.scopeIdSet.contains(idUtils.getTableId(targetId));
        }
        return false;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Experiment getExperiment() {
        return this.experiment;
    }

    /*
     * @Override public boolean equals(Object obj) { ensureConstraintsLoaded(); if (obj instanceof
     * RDBMSConstraintCollection) { ((RDBMSConstraintCollection) obj).ensureConstraintsLoaded(); } return
     * super.equals(obj); }
     * @Override public int hashCode() { ensureConstraintsLoaded(); return super.hashCode(); }
     */

    @Override
    //public Class<T> getConstraintClass(){
    //  return this.constrainttype;
    //}

    public Class<T> getConstraintClass() {
        if (this.constrainttype == null) {
            for (T constraint : getConstraints()) {
                this.constrainttype = (Class<T>) constraint.getClass();
                break;
            }
        }
        return this.constrainttype;
    }
}
