package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * This class is a {@link de.hpi.isg.mdms.model.constraints.Constraint} representing the data type of a certain {@link Column}. {@link Column}.
 */
public class TypeConstraint extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeConstraint.class);

    private static final long serialVersionUID = 3194245498846860560L;

    private final String type;

    private final SingleTargetReference target;

    @Deprecated
    public static TypeConstraint build(final SingleTargetReference target, String type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, type);
        return typeConstraint;
    }

    public static TypeConstraint buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection<TypeConstraint> constraintCollection,
            String type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, type);
        constraintCollection.add(typeConstraint);
        return typeConstraint;
    }

    public TypeConstraint(final SingleTargetReference target, String type) {
        Validate.isTrue(target.getAllTargetIds().size() == 1);

        this.type = type;
        this.target = target;
    }

    @Override
    public String toString() {
        return "TypeConstraint [type=" + type + "]";
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.target;
    }

    public String getType() {
        return type;
    }

}
