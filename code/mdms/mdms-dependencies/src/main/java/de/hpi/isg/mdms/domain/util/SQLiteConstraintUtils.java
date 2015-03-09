package de.hpi.isg.mdms.domain.util;

import de.hpi.isg.mdms.domain.constraints.impl.*;
import de.hpi.isg.mdms.factories.SQLiteInterface;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * This class provides utility methods to work with SQLiteInterfaces.
 *
 * @author Sebastian
 * @since 04.03.2015.
 */
public class SQLiteConstraintUtils {


    /**
     * Registers the standard constraint serializers for SQLite with the given SQLite interface.
     * @param sqliteInterface is the SQLiteInterface with that the serializers shall be registered
     * @return the given interface
     */
    public static SQLiteInterface registerStandardConstraints(SQLiteInterface sqliteInterface) {
        sqliteInterface.registerConstraintSQLSerializer(DistinctValueCount.class,
                new DistinctValueCount.DistinctValueCountSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(InclusionDependency.class,
                new InclusionDependency.InclusionDependencySQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(TupleCount.class, new TupleCount.TupleCountSQLiteSerializer(
                sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(TypeConstraint.class,
                new TypeConstraint.TypeConstraintSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(UniqueColumnCombination.class, new
                UniqueColumnCombination.UniqueColumnCombinationSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(DistinctValueOverlap.class, new
                DistinctValueOverlap.DistinctValueOverlapSQLiteSerializer(sqliteInterface));

        return sqliteInterface;
    }




}
