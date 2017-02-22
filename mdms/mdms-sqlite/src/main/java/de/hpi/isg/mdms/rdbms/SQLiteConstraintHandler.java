package de.hpi.isg.mdms.rdbms;

import com.twitter.chill.KryoPool;
import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.RDBMSConstraintCollection;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.util.LRUCache;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * This class takes care of serializing and deserializing constraints on a SQLite database.
 *
 * @author sebastian.kruse
 * @since 10.03.2015
 */
public class SQLiteConstraintHandler {

    /**
     * Encapsulates the DB connection to allow for lazy writes.
     */
    private final DatabaseAccess databaseAccess;

    /**
     * Used to serialize BLOBs.
     */
    private final KryoPool kryoPool;

    /**
     * The {@link de.hpi.isg.mdms.rdbms.SQLiteInterface} for that this manager works.
     */
    private final SQLiteInterface sqliteInterface;

    RDBMSMetadataStore metadataStore;

    private int currentConstraintIdMax = -1;

    private final DatabaseWriter<ConstraintCollection<? extends Constraint>> addConstraintCollectionWriter;
    private final DatabaseWriter<ConstraintCollection<?>> deleteConstraintCollectionWriter;
    private final DatabaseQuery<Integer> constraintCollectionByIdQuery;
    private final DatabaseQuery<Void> allConstraintCollectionsQuery;
    private final LRUCache<Integer, RDBMSConstraintCollection<? extends Constraint>> constraintCollectionCache = new LRUCache<>(100);
    private boolean isConstraintCollectionCacheComplete = false;

    private final DatabaseWriter<Tuple2<ConstraintCollection<?>, Constraint>> addConstraintWriter;
    private final DatabaseWriter<ConstraintCollection<?>> deleteConstraintsWriter;
    private final DatabaseQuery<Integer> constraintsByConstraintCollectionIdQuery;


    /**
     * Creates a new instance.
     *
     * @param sqliteInterface is the SQLiteInterface for that this instance operates
     */
    public SQLiteConstraintHandler(SQLiteInterface sqliteInterface, KryoPool kryoPool) throws SQLException {
        this.sqliteInterface = sqliteInterface;
        this.databaseAccess = sqliteInterface.getDatabaseAccess();
        this.kryoPool = kryoPool;

        this.addConstraintCollectionWriter = this.databaseAccess.createBatchWriter(
                new PreparedStatementBatchWriter.Factory<ConstraintCollection<? extends Constraint>>(
                        "insert into [ConstraintCollection] ([id], [experimentId], [description], [data]) values (?, ?, ?, ?)",
                        (cc, preparedStatement) -> {
                            preparedStatement.setInt(1, cc.getId());
                            if (cc.getExperiment() == null) {
                                preparedStatement.setNull(2, Types.INTEGER);
                            } else {
                                preparedStatement.setInt(2, cc.getExperiment().getId());
                            }
                            preparedStatement.setString(3, cc.getDescription());
                            preparedStatement.setBytes(
                                    4,
                                    this.kryoPool.toBytesWithoutClass(new SQLiteConstraintHandler.ConstraintCollectionData(cc))
                            );
                        },
                        "ConstraintCollection"
                ));
        this.deleteConstraintCollectionWriter = this.databaseAccess.createBatchWriter(
                new PreparedStatementBatchWriter.Factory<ConstraintCollection<?>>(
                        "delete from [ConstraintCollection] where [id]=?",
                        (cc, preparedStatement) -> preparedStatement.setInt(1, cc.getId()),
                        "ConstraintCollection"
                )
        );
        this.constraintCollectionByIdQuery = this.databaseAccess.createQuery(new StrategyBasedPreparedQuery.Factory<>(
                "select * from [ConstraintCollection] where [id]=?",
                PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                "ConstraintCollection"
        ));
        this.allConstraintCollectionsQuery = this.databaseAccess.createQuery(new StrategyBasedPreparedQuery.Factory<>(
                "select * from [ConstraintCollection]",
                PreparedStatementAdapter.VOID_ADAPTER,
                "ConstraintCollection"
        ));

        this.addConstraintWriter = this.databaseAccess.createBatchWriter(
                new PreparedStatementBatchWriter.Factory<Tuple2<ConstraintCollection<?>, Constraint>>(
                        "insert into [Constraint] ([constraintCollection], [data]) values (?, ?)",
                        (params, preparedStatement) -> {
                            preparedStatement.setInt(1, params._1().getId());
                            preparedStatement.setBytes(2, this.kryoPool.toBytesWithoutClass(params._2()));
                        },
                        "Constraint"
                ));
        this.deleteConstraintsWriter = this.databaseAccess.createBatchWriter(
                new PreparedStatementBatchWriter.Factory<ConstraintCollection<?>>(
                        "delete from [Constraint] where [constraintCollection]=?",
                        (cc, preparedStatement) -> preparedStatement.setInt(1, cc.getId()),
                        "Constraint"
                ));
        this.constraintsByConstraintCollectionIdQuery = this.databaseAccess.createQuery(
                new StrategyBasedPreparedQuery.Factory<>(
                        "select [data] from [Constraint] where [constraintCollection]=?",
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        "Constraint"
                )
        );
    }


    /**
     * Writes a constraint to the DB.
     *
     * @param constraint           is a constraint that shall be written
     * @param constraintCollection to which the {@code constraint} should belong
     */
    public void writeConstraint(Constraint constraint, RDBMSConstraintCollection<? extends Constraint> constraintCollection) throws SQLException {
        Validate.isAssignableFrom(constraintCollection.getConstraintClass(), constraint.getClass());
        this.addConstraintWriter.write(new Tuple2<>(constraintCollection, constraint));
    }

    /**
     * Loads a constraint collection with the given ID. The scope is not loaded, though.
     *
     * @param id is the ID of the collection
     * @return the loaded collection or {@code null} if there is no constraint collection with the associated ID
     */
    @SuppressWarnings("unchecked")
    public RDBMSConstraintCollection<?> getConstraintCollectionById(int id) throws SQLException {
        RDBMSConstraintCollection<?> cc = this.constraintCollectionCache.get(id);
        if (cc != null) return cc;

        try (ResultSet rs = this.constraintCollectionByIdQuery.execute(id)) {
            if (rs.next()) {
                Validate.isTrue(id == rs.getInt(1));

                int experimentId = rs.getInt(2);
                Experiment experiment = this.metadataStore.getExperimentById(experimentId);

                String description = rs.getString(3);

                SQLiteConstraintHandler.ConstraintCollectionData data = this.kryoPool.fromBytes(
                        rs.getBytes(4), SQLiteConstraintHandler.ConstraintCollectionData.class
                );
                Set<Target> scope = new HashSet<>(data.scopeIds.length);
                for (int scopeId : data.scopeIds) {
                    scope.add(this.metadataStore.getTargetById(scopeId));
                }

                cc = new RDBMSConstraintCollection<>(
                        id, description, experiment, scope, this.sqliteInterface, (Class<Constraint>) data.constraintClass
                );
                return cc;
            }
        }

        return null;
    }

    /**
     * Loads all constraint collections. The scopes are not loaded, though.
     *
     * @return the loaded collections
     */
    public Collection<ConstraintCollection<? extends Constraint>> getAllConstraintCollections() throws SQLException {
        if (this.isConstraintCollectionCacheComplete) {
            return new ArrayList<>(this.constraintCollectionCache.values());
        }

        this.constraintCollectionCache.setEvictionEnabled(false);
        try (ResultSet rs = this.allConstraintCollectionsQuery.execute(null)) {
            while (rs.next()) {
                Integer id = rs.getInt(1);
                if (this.constraintCollectionCache.containsKey(id)) continue;

                int experimentId = rs.getInt(2);
                Experiment experiment = this.metadataStore.getExperimentById(experimentId);

                String description = rs.getString(3);

                SQLiteConstraintHandler.ConstraintCollectionData data = this.kryoPool.fromBytes(
                        rs.getBytes(4), SQLiteConstraintHandler.ConstraintCollectionData.class
                );
                Set<Target> scope = new HashSet<>(data.scopeIds.length);
                for (int scopeId : data.scopeIds) {
                    scope.add(this.metadataStore.getTargetById(scopeId));
                }

                RDBMSConstraintCollection<?> cc = new RDBMSConstraintCollection<>(
                        id, description, experiment, scope, this.sqliteInterface, (Class<Constraint>) data.constraintClass
                );
                this.constraintCollectionCache.put(id, cc);
            }
        }

        this.isConstraintCollectionCacheComplete = true;
        return new ArrayList<>(this.constraintCollectionCache.values());
    }


    public void addConstraintCollection(ConstraintCollection<? extends Constraint> constraintCollection) throws SQLException {
        this.addConstraintCollectionWriter.write(constraintCollection);
    }

    @SuppressWarnings("unchecked") // We check by hand.
    public <T extends Constraint> Collection<T> getAllConstraintsForConstraintCollection(
            ConstraintCollection<?> constraintCollection) throws Exception {
        Collection<T> constraints = new HashSet<>();
        try {
            this.metadataStore.flush();
        } catch (Exception e) {
            throw new SQLException("Could not flush metadata store prior to reading constraints.", e);
        }

        try (ResultSet rs = this.constraintsByConstraintCollectionIdQuery.execute(constraintCollection.getId())) {
            while (rs.next()) {
                Constraint constraint = this.kryoPool.fromBytes(
                        rs.getBytes(1),
                        constraintCollection.getConstraintClass()
                );
                Validate.isAssignableFrom(constraintCollection.getConstraintClass(), constraint.getClass());
                constraints.add((T) constraint);
            }
        }
        return constraints;
    }

    public void removeConstraintCollection(ConstraintCollection<? extends Constraint> constraintCollection) throws SQLException {
        // We need to avoid to batch inserts and deletes together.
        this.databaseAccess.flush(Arrays.asList("Constraint", "ConstraintCollection"));

        // Remove the ConstraintCollection from the cache.
        this.constraintCollectionCache.remove(constraintCollection.getId());

        // Remove the ConstraintCollection from the database.
        this.deleteConstraintCollectionWriter.write(constraintCollection);
        this.deleteConstraintsWriter.write(constraintCollection);

        this.databaseAccess.flush(Arrays.asList("Constraint", "ConstraintCollection"));
    }

    public Set<ConstraintCollection<? extends Constraint>> getAllConstraintCollectionsForExperiment(Experiment experiment) {
//        try {
//            Set<ConstraintCollection<? extends Constraint>> constraintCollections = new HashSet<>();
//
//            String sqlconstraintCollectionsForExperiment = String
//                    .format("SELECT constraintCollection.id as id from constraintCollection where constraintCollection.experimentId = %d;",
//                            experiment.getId());
//
//            ResultSet rs = databaseAccess.query(sqlconstraintCollectionsForExperiment, "constraintCollection");
//            while (rs.next()) {
//                constraintCollections.add(getConstraintCollectionById(rs.getInt("id")));
//            }
//            rs.close();
//            return constraintCollections;
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        throw new UnsupportedOperationException("Not implemented.");
    }


    public void setMetadataStore(RDBMSMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Captures BLOB data for {@link ConstraintCollection}s.
     */
    private static class ConstraintCollectionData {

        private Class<?> constraintClass;

        private int[] scopeIds;

        private ConstraintCollectionData(ConstraintCollection<?> constraintCollection) {
            this.constraintClass = constraintCollection.getConstraintClass();
            this.scopeIds = new int[constraintCollection.getScope().size()];
            int i = 0;
            for (Target target : constraintCollection.getScope()) {
                this.scopeIds[i++] = target.getId();
            }
        }

        /**
         * De-serialization constructor.
         */
        private ConstraintCollectionData() {
        }

    }
}
