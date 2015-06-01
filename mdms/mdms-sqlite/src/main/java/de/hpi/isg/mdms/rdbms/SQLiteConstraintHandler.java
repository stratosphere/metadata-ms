package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraint;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Target;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * This class takes care of serializing and deserializing constraints on a SQLite database.
 *
 * @author sebastian.kruse
 * @since 10.03.2015
 */
public class SQLiteConstraintHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SQLiteConstraintHandler.class);

    private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_CONSTRAINT_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Constraintt (id, constraintCollectionId) VALUES (?, ?);",
                    new PreparedStatementAdapter<int[]>() {
                        @Override
                        public void translateParameter(int[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters[0]);
                            preparedStatement.setInt(2, parameters[1]);
                        }
                    },
                    "Constraintt");

    private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_CONSTRAINT_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE from Constraintt where constraintCollectionId=?;",
                    new PreparedStatementAdapter<Integer>() {
                        @Override
                        public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter);
                        }
                    },
                    "Constraintt");

    private final Map<Class<? extends Constraint>, ConstraintSQLSerializer<? extends Constraint>> constraintSerializers = new HashMap<>();

    /**
     * Encapsulates the DB connection to allow for lazy writes.
     */
    private final DatabaseAccess databaseAccess;

    /**
     * The {@link de.hpi.isg.mdms.rdbms.SQLiteInterface} for that this manager works.
     */
    private final SQLiteInterface sqliteInterface;

    RDBMSMetadataStore metadataStore;

    private int currentConstraintIdMax = -1;

    private DatabaseWriter<int[]> insertConstraintWriter;

    private DatabaseWriter<Integer> deleteConstraintWriter;

    /**
     * Creates a new instance.
     *
     * @param sqliteInterface is the SQLiteInterface for that this instance operates
     */
    public SQLiteConstraintHandler(SQLiteInterface sqliteInterface) {
        this.sqliteInterface = sqliteInterface;
        this.databaseAccess = sqliteInterface.getDatabaseAccess();

        // Initialize writers and queries.
        try {
            // Writers
            this.insertConstraintWriter = this.databaseAccess.createBatchWriter(INSERT_CONSTRAINT_WRITER_FACTORY);
            this.deleteConstraintWriter = this.databaseAccess.createBatchWriter(DELETE_CONSTRAINT_WRITER_FACTORY);
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize writers.", e);
        }
    }

    /**
     * Writes a constraint to the DB.
     *
     * @param constraint is an {@link de.hpi.isg.mdms.domain.constraints.RDBMSConstraint} that should be written
     */
    public void writeConstraint(Constraint constraint) {
        if (!(constraint instanceof RDBMSConstraint)) {
            throw new IllegalArgumentException("Not an RDBMSConstraint: " + constraint);
        }

        writeConstraint((RDBMSConstraint) constraint);
    }

    /**
     * Writes a constraint to the DB.
     *
     * @param constraint is a constraint that shall be written
     */
    public void writeConstraint(RDBMSConstraint constraint) {
        ensureCurrentConstraintIdMaxInitialized();

        // for auto-increment id
        Integer constraintId = ++currentConstraintIdMax;
        try {
            this.insertConstraintWriter.write(new int[]{constraintId, constraint.getConstraintCollection().getId()});
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Try to find an existing serializer for the constraint type.
        ConstraintSQLSerializer<? extends Constraint> serializer = constraintSerializers.get(constraint.getClass());

        // If there is no serializer, create a new one.
        if (serializer == null) {
            serializer = constraint.getConstraintSQLSerializer(this.sqliteInterface);
            constraintSerializers.put(constraint.getClass(), serializer);
            serializer.initializeTables();
        }

        // Delegate the serialization.
        serializer.serialize(constraintId, constraint);
    }

    /**
     * Checks if {@link #currentConstraintIdMax} already has a valid value. If not, a valid value is set.
     */
    private void ensureCurrentConstraintIdMaxInitialized() {
        if (this.currentConstraintIdMax != -1) {
            return;
        }

        try {
            this.currentConstraintIdMax = 0;
            try (ResultSet res = this.databaseAccess.query("SELECT MAX(id) from Constraintt;", "Constraintt")) {
                while (res.next()) {
                    this.currentConstraintIdMax = res.getInt("max(id)");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads all constraint collections. The scopes are not loaded, though.
     *
     * @return the loaded collections
     */
    public Collection<RDBMSConstraintCollection> getAllConstraintCollections() {
        try {
            // TODO: This seems not to be working (only a single constraint collection is returned.
            Collection<RDBMSConstraintCollection> constraintCollections = new LinkedList<>();
            try (ResultSet rs = this.databaseAccess.query("SELECT id, description, experimentId from ConstraintCollection;",
                    "ConstraintCollection")) {
                while (rs.next()) {
                    RDBMSConstraintCollection constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"),
                            rs.getString("description"), this.sqliteInterface.getExperimentById(rs.getInt("experimentId")),
                            this.sqliteInterface);
                    constraintCollections.add(constraintCollection);
                }
            }
            return constraintCollections;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads a constraint collection with the given ID. The scope is not loaded, though.
     *
     * @param id is the ID of the collection
     * @return the loaded collection or {@code null} if there is no constraint collection with the associated ID
     */
    public RDBMSConstraintCollection getConstraintCollectionById(int id) {
        try {
            RDBMSConstraintCollection constraintCollection = null;
            String getConstraintCollectionByIdQuery =
                    String.format("SELECT id, description, experimentId from ConstraintCollection where id=%d;", id);
            try (ResultSet rs = this.databaseAccess.query(getConstraintCollectionByIdQuery, "ConstraintCollection")) {
                while (rs.next()) {
                    constraintCollection = new RDBMSConstraintCollection(rs.getInt("id"), rs.getString("description"), this.sqliteInterface.getExperimentById(rs.getInt("experimentId")),
                            this.sqliteInterface);
                }
            }
            return constraintCollection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Returns the IDs of the schema elements that form the scope of a constraint collection.
     *
     * @param rdbmsConstraintCollection is a constraint collection whose scope is sought
     * @return the IDs of the schema elements in the scope
     */
    public IntCollection getScopeOfConstraintCollectionAsIds(RDBMSConstraintCollection rdbmsConstraintCollection) {
        try {
            IntCollection ids = new IntArrayList();
            String sqlGetScope = String
                    .format("SELECT id from target, scope where scope.targetId = target.id and scope.constraintCollectionId=%d;",
                            rdbmsConstraintCollection.getId());
            try (ResultSet rs = this.databaseAccess.query(sqlGetScope, "Target", "Scope")) {
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                }
            }
            return ids;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void addConstraintCollection(ConstraintCollection constraintCollection) {
        try {
        	if (constraintCollection.getExperiment() == null) {
                String sqlAddConstraintCollection = String.format(
                        "INSERT INTO ConstraintCollection (id, description) VALUES (%d, '%s');",
                        constraintCollection.getId(), constraintCollection.getDescription());
                this.databaseAccess.executeSQL(sqlAddConstraintCollection, "ConstraintCollection");	
        	} else {
                String sqlAddConstraintCollection = String.format(
                        "INSERT INTO ConstraintCollection (id, experimentId, description) VALUES (%d, %d, '%s');",
                        constraintCollection.getId(), constraintCollection.getExperiment().getId(), constraintCollection.getDescription());
                this.databaseAccess.executeSQL(sqlAddConstraintCollection, "ConstraintCollection");
        	}
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void addScope(Target target, ConstraintCollection constraintCollection) {
        try {
            String sqlAddScope = String.format("INSERT INTO Scope (targetId, constraintCollectionId) VALUES (%d, %d);",
                    target.getId(), constraintCollection.getId());
            this.databaseAccess.executeSQL(sqlAddScope, "Scope");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<Constraint> getAllConstraintsForConstraintCollection(
            RDBMSConstraintCollection rdbmsConstraintCollection) {

        Collection<Constraint> constraintsOfCollection = new HashSet<>();

        try {
            this.metadataStore.flush();
        } catch (Exception e) {
            throw new RuntimeException("Could not flush metadata metadataStore before loading constraints.", e);
        }
        for (ConstraintSQLSerializer<? extends Constraint> constraintSerializer : this.constraintSerializers.values()) {
            try {
                constraintsOfCollection.addAll(constraintSerializer
                        .deserializeConstraintsOfConstraintCollection(rdbmsConstraintCollection));
            } catch (Exception e) {
                LOG.error("Error on deserializing constraint collection. Continue anyway...", e);
            }
        }

        if (constraintsOfCollection.isEmpty()) {
            LOG.warn(
                    "Could not find constraints for constraint collection {}. Did you register the constraint type properly?",
                    rdbmsConstraintCollection != null ? rdbmsConstraintCollection.getId() : "");
        }

        return constraintsOfCollection;
    }


    public void registerConstraintSQLSerializer(Class<? extends Constraint> clazz,
                                                ConstraintSQLSerializer<? extends Constraint> serializer) {
        constraintSerializers.put(clazz, serializer);
        serializer.initializeTables();
    }


    public void removeConstraintCollection(ConstraintCollection constraintCollection) {
        try {
            this.databaseAccess.flush();
            for (ConstraintSQLSerializer<? extends Constraint> constraintSerializer : this.constraintSerializers
                    .values()) {
                constraintSerializer
                        .removeConstraintsOfConstraintCollection(constraintCollection);

            }

            String sqlDeleteScope = String.format("DELETE from Scope where constraintCollectionId=%d;",
                    constraintCollection.getId());
            this.databaseAccess.executeSQL(sqlDeleteScope, "Scope");

            String sqlDeleteConstraintCollection = String.format(
                    "DELETE from ConstraintCollection where id=%d;",
                    constraintCollection.getId());
            this.databaseAccess.executeSQL(sqlDeleteConstraintCollection, "ConstraintCollection");

            this.deleteConstraintWriter.write(constraintCollection.getId());

            this.databaseAccess.flush();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

	public Set<ConstraintCollection> getAllConstraintCollectionsForExperiment(Experiment experiment) {
        try {
            Set<ConstraintCollection> constraintCollections = new HashSet<>();

            String sqlconstraintCollectionsForExperiment = String
                    .format("SELECT constraintCollection.id as id from constraintCollection where constraintCollection.experimentId = %d;",
                            experiment.getId());

            ResultSet rs = databaseAccess.query(sqlconstraintCollectionsForExperiment, "constraintCollection");
            while (rs.next()) {
                constraintCollections.add(getConstraintCollectionById(rs.getInt("id")));
            }
            rs.close();
            return constraintCollections;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

	}
	
    public void initializeTables() {
        // init constraint types
        for (ConstraintSQLSerializer<? extends Constraint> serializer : this.constraintSerializers.values()) {
            serializer.initializeTables();
        }
    }

    /**
     * Drop the tables used by the constraint serializers.
     *
     * @param statement is provided to execute SQL
     */
    void dropConstraintTables(Statement statement) throws SQLException {
        for (ConstraintSQLSerializer<?> serializer : this.constraintSerializers.values()) {
            for (String tableName : serializer.getTableNames()) {
                // toLowerCase because SQLite is case-insensitive for table names
                String sql = String.format("DROP TABLE IF EXISTS [%s];", tableName);
                statement.execute(sql);
            }
        }
    }

    public void setMetadataStore(RDBMSMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
}
