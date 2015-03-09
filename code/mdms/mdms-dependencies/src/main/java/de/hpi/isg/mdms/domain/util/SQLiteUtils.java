package de.hpi.isg.mdms.domain.util;

import de.hpi.isg.mdms.domain.constraints.impl.*;
import de.hpi.isg.mdms.factories.SQLiteInterface;

import java.sql.Connection;

/**
 * @author Sebastian
 * @since 04.03.2015.
 */
public class SQLiteUtils {


    public static SQLiteInterface buildAndRegisterStandardConstraints(Connection connection) {
        SQLiteInterface sqlInterface = new SQLiteInterface(connection);
        sqlInterface.registerConstraintSQLSerializer(DistinctValueCount.class,
                new DistinctValueCount.DistinctValueCountSQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(InclusionDependency.class,
                new InclusionDependency.InclusionDependencySQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(TupleCount.class, new TupleCount.TupleCountSQLiteSerializer(
                sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(TypeConstraint.class,
                new TypeConstraint.TypeConstraintSQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(UniqueColumnCombination.class, new
                UniqueColumnCombination.UniqueColumnCombinationSQLiteSerializer(sqlInterface));
        sqlInterface.registerConstraintSQLSerializer(DistinctValueOverlap.class, new
                DistinctValueOverlap.DistinctValueOverlapSQLiteSerializer(sqlInterface));

        return sqlInterface;
    }

}
