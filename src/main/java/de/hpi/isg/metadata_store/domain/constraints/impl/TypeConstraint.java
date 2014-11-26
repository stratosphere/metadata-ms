package de.hpi.isg.metadata_store.domain.constraints.impl;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.ConstraintCollection;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.TargetReference;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * This class is a {@link Constraint} representing the data type of a certain {@link Column}. {@link Column}.
 */
public class TypeConstraint extends AbstractConstraint implements Constraint {

    public enum TYPES {
        STRING, INTEGER, DECIMAL
    };

    private class TypeConstraintSQLiteSerializer implements ConstraintSQLSerializer {

        private boolean allTablesExistChecked = false;

        private final static String tableName = "Typee";

        private final SQLInterface sqlInterface;

        public TypeConstraintSQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;
        }

        @Override
        public void serialize(Integer constraintId, Constraint typeConstraint) {
            Validate.isTrue(typeConstraint instanceof TypeConstraint);
            try {
                Statement stmt = sqlInterface.createStatement();
                if (!allTablesExistChecked) {
                    if (!sqlInterface.tableExists(tableName)) {
                        String createTypeeTable = "CREATE TABLE [Typee]\n" +
                                "(\n" +
                                "    [constraintId] integer NOT NULL,\n" +
                                "    [columnId] integer NOT NULL,\n" +
                                "    [typee] text,\n" +
                                "    FOREIGN KEY ([constraintId])\n" +
                                "    REFERENCES [Constraintt] ([id]),\n" +
                                "    FOREIGN KEY ([columnId])\n" +
                                "    REFERENCES [Columnn] ([id])\n" +
                                ");";
                        this.sqlInterface.executeCreateTableStatement(createTypeeTable);
                    }
                    if (sqlInterface.tableExists(tableName)) {
                        this.allTablesExistChecked = true;
                    }
                }

                String sqlAddTypee = String.format(
                        "INSERT INTO Typee (constraintId, typee, columnId) VALUES (%d, '%s', %d);",
                        constraintId, ((TypeConstraint) typeConstraint).getType().name(), typeConstraint
                                .getTargetReference()
                                .getAllTargets().iterator()
                                .next().getId());
                stmt.executeUpdate(sqlAddTypee);

                stmt.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }

    private static final long serialVersionUID = 3194245498846860560L;

    private final TYPES type;

    private final TargetReference target;

    public static TypeConstraint build(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            TYPES type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, constraintCollection, type);
        return typeConstraint;
    }

    public static TypeConstraint buildAndAddToCollection(final SingleTargetReference target,
            ConstraintCollection constraintCollection,
            TYPES type) {
        TypeConstraint typeConstraint = new TypeConstraint(target, constraintCollection, type);
        constraintCollection.add(typeConstraint);
        return typeConstraint;
    }

    private TypeConstraint(final SingleTargetReference target, ConstraintCollection constraintCollection,
            TYPES type) {
        super(constraintCollection);
        Validate.isTrue(target.getAllTargets().size() == 1);
        for (final Target t : target.getAllTargets()) {
            if (!(t instanceof Column)) {
                throw new IllegalArgumentException("TypeConstrains can only be defined on Columns. But target was: "
                        + t);
            }
        }
        this.type = type;
        this.target = target;
    }

    @Override
    public String toString() {
        return "TypeConstraint [type=" + type + "]";
    }

    @Override
    public TargetReference getTargetReference() {
        return target;
    }

    public TYPES getType() {
        return type;
    }

    @Override
    public ConstraintSQLSerializer getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new TypeConstraintSQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }
}
