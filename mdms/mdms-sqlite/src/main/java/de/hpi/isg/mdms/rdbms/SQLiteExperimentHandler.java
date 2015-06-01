package de.hpi.isg.mdms.rdbms;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;






import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.experiment.RDBMSAlgorithm;
import de.hpi.isg.mdms.domain.experiment.RDBMSExperiment;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Annotation;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.util.LRUCache;

/**
 * This class takes care of serializing and deserializing experiments on a SQLite database.
 *
 * @author susanne
 */

public class SQLiteExperimentHandler {
	
    /**
     * Encapsulates the DB connection to allow for lazy writes.
     */
    private final DatabaseAccess databaseAccess;
 
    /**
     * The {@link de.hpi.isg.mdms.rdbms.SQLiteInterface} for that this manager works.
     */
    private final SQLiteInterface sqliteInterface;

    RDBMSMetadataStore metadataStore;

    private DatabaseWriter<RDBMSAlgorithm> insertAlgorithmWriter;
    private DatabaseWriter<Algorithm> deleteAlgorithmWriter;
    private DatabaseQuery<Integer> algorithmQuery;
    private DatabaseQuery<String> algorithmQueryName;
    
    private DatabaseWriter<RDBMSExperiment> insertExperimentWriter;
    private DatabaseWriter<Experiment> deleteExperimentWriter;
    private DatabaseQuery<Integer> experimentQuery;
    private DatabaseWriter<Object[]> updateExperimentWriter;
    
    private DatabaseWriter<Object[]> insertParameterWriter;
    private DatabaseWriter<Integer> deleteParameterWriter;
    private DatabaseQuery<Integer> parameterQuery;
    
    private DatabaseWriter<Object[]> insertAnnotationWriter;
    private DatabaseWriter<Integer> deleteAnnotationWriter;
    private DatabaseQuery<Integer> annotationQuery;
    
    private static final PreparedStatementBatchWriter.Factory<RDBMSAlgorithm> INSERT_ALGORITHM_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Algorithm (id, name) VALUES (?, ?);",
                    new PreparedStatementAdapter<RDBMSAlgorithm>() {
                        @Override
                        public void translateParameter(RDBMSAlgorithm parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameters.getId());
                            preparedStatement.setString(2, parameters.getName());
                        }
                    },
                    "Algorithm");
    
    private static final PreparedStatementBatchWriter.Factory<Algorithm> DELETE_ALGORITHM_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "DELETE FROM Algorithm where id=?;",
                    new PreparedStatementAdapter<Algorithm>() {
                        @Override
                        public void translateParameter(Algorithm parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setInt(1, parameter.getId());
                        }
                    },
                    "Algorithm");

        private static final PreparedStatementBatchWriter.Factory<RDBMSExperiment> INSERT_EXPERIMENT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO Experiment (id, description, timestamp, executionTime, algorithmId) VALUES (?, ?, ?,?,?);",
                        new PreparedStatementAdapter<RDBMSExperiment>() {
                            @Override
                            public void translateParameter(RDBMSExperiment parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameters.getId());
                                preparedStatement.setString(2, parameters.getDescription());
                                preparedStatement.setString(3, parameters.getTimestamp());
                                preparedStatement.setInt(4, parameters.getExecutionTime().intValue());
                                preparedStatement.setInt(5, parameters.getAlgorithm().getId());
                            }
                        },
                        "Experiment");

        private static final PreparedStatementBatchWriter.Factory<Object[]> UPDATE_EXPERIMENT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "UPDATE Experiment"
                        + " SET executionTime=? WHERE id=?;",
                        new PreparedStatementAdapter<Object[]>() {
                            @Override
                            public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(2, (Integer) parameters[0]);
                                preparedStatement.setInt(1, ((Long) parameters[1]).intValue());
                            }
                        },
                        "Experiment");
                
        private static final PreparedStatementBatchWriter.Factory<Experiment> DELETE_EXPERIMENT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE FROM Experiment where id=?;",
                        new PreparedStatementAdapter<Experiment>() {
                            @Override
                            public void translateParameter(Experiment parameter, PreparedStatement preparedStatement)
                                    throws SQLException {
                                preparedStatement.setInt(1, parameter.getId());
                            }
                        },
                        "Experiment");

            private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_EXPERIMENT_PARAMETER_WRITER_FACTORY =
                    new PreparedStatementBatchWriter.Factory<>(
                            "INSERT INTO ExperimentParameter (experimentId, keyy, value) VALUES (?, ?, ?);",
                            new PreparedStatementAdapter<Object[]>() {
                                @Override
                                public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                        throws SQLException {
                                    preparedStatement.setInt(1, (Integer) parameters[0]);
                                    preparedStatement.setString(2, (String) parameters[1]);
                                    preparedStatement.setString(3, (String) parameters[2]);
                                }
                            },
                            "ExperimentParameter");

            private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_EXPERIMENT_PARAMETER_WRITER_FACTORY =
                    new PreparedStatementBatchWriter.Factory<>(
                            "DELETE FROM ExperimentParameter where experimentId=?;",
                            new PreparedStatementAdapter<Integer>() {
                                @Override
                                public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                        throws SQLException {
                                    preparedStatement.setInt(1, parameter);
                                }
                            },
                            "ExperimentParameter");

            private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_EXPERIMENT_ANNOTATION_WRITER_FACTORY =
                    new PreparedStatementBatchWriter.Factory<>(
                            "INSERT INTO Annotation (experimentId, tag, textt) VALUES (?, ?, ?);",
                            new PreparedStatementAdapter<Object[]>() {
                                @Override
                                public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                        throws SQLException {
                                    preparedStatement.setInt(1, (Integer) parameters[0]);
                                    preparedStatement.setString(2, (String) parameters[1]);
                                    preparedStatement.setString(3, (String) parameters[2]);
                                }
                            },
                            "Annotation");

            private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_EXPERIMENT_ANNOTATION_WRITER_FACTORY =
                    new PreparedStatementBatchWriter.Factory<>(
                            "DELETE FROM Annotation where experimentId=?;",
                            new PreparedStatementAdapter<Integer>() {
                                @Override
                                public void translateParameter(Integer parameter, PreparedStatement preparedStatement)
                                        throws SQLException {
                                    preparedStatement.setInt(1, parameter);
                                }
                            },
                            "Annotation");

           private static final StrategyBasedPreparedQuery.Factory<Integer> EXPERIMENT_QUERY_FACTORY =
                    new StrategyBasedPreparedQuery.Factory<>(
                            "SELECT experiment.id as id, experiment.timestamp as timestamp, experiment.executionTime as executionTime,"
                            + " experiment.algorithmId as algorithmId, experiment.description as description"
                                    + " from experiment"
                                    + " where experiment.id=?",
                            PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                            "Experiment");

            private static final StrategyBasedPreparedQuery.Factory<Integer> ALGORITHM_QUERY_FACTORY =
                    new StrategyBasedPreparedQuery.Factory<>(
                            "SELECT algorithm.id as id, algorithm.name as name"
                                    + " from algorithm"
                                    + " where algorithm.id=?",
                            PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                            "Algorithm");

            private static final StrategyBasedPreparedQuery.Factory<String> ALGORITHM_NAME_QUERY_FACTORY =
            new StrategyBasedPreparedQuery.Factory<>(
                    "SELECT algorithm.id as id, algorithm.name as name"
                            + " from algorithm"
                            + " where algorithm.name=?",
                    new PreparedStatementAdapter<String>() {
                        @Override
                        public void translateParameter(String parameter, PreparedStatement preparedStatement)
                                throws SQLException {
                            preparedStatement.setString(1, parameter);
                        }
                    },
                    "Algorithm");
                    
            private static final StrategyBasedPreparedQuery.Factory<Integer> PARAMETER_QUERY_FACTORY =
                    new StrategyBasedPreparedQuery.Factory<>(
                            "SELECT ExperimentParameter.experimentId as experimentId, ExperimentParameter.keyy as keyy,"
                            + " ExperimentParameter.value as value"
                                    + " from ExperimentParameter"
                                    + " where ExperimentParameter.experimentId=?",
                            PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                            "ExperimentParameter");

                    private static final StrategyBasedPreparedQuery.Factory<Integer> ANNOTATION_QUERY_FACTORY =
                            new StrategyBasedPreparedQuery.Factory<>(
                                    "SELECT Annotation.experimentId as experimentId, Annotation.tag as tag, Annotation.textt as textt"
                                            + " from Annotation"
                                            + " where Annotation.experimentId=?",
                                    PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                                    "Annotation");
                            
           private final static int CACHE_SIZE = 1000;
           
           LRUCache<Algorithm, Collection<Experiment>> allExperimentsForAlgorithmCache = new LRUCache<>(CACHE_SIZE);
           LRUCache<Experiment, Collection<ConstraintCollection>> allConstraintCollectionsForExperimentCache = new LRUCache<>(CACHE_SIZE);
           LRUCache<Integer, RDBMSExperiment> experimentCache = new LRUCache<>(CACHE_SIZE);
           LRUCache<Integer, RDBMSAlgorithm> algorithmCache = new LRUCache<>(CACHE_SIZE);
                    
    /**
     * Creates a new instance.
     *
     * @param sqliteInterface is the SQLiteInterface for that this instance operates
     */
    public SQLiteExperimentHandler(SQLiteInterface sqliteInterface) {
        this.sqliteInterface = sqliteInterface;
        this.databaseAccess = sqliteInterface.getDatabaseAccess();

        // Initialize writers and queries.
        try {
            // Writers
            this.insertAlgorithmWriter = this.databaseAccess.createBatchWriter(INSERT_ALGORITHM_WRITER_FACTORY);
            this.deleteAlgorithmWriter = this.databaseAccess.createBatchWriter(DELETE_ALGORITHM_WRITER_FACTORY);
            this.algorithmQuery = this.databaseAccess.createQuery(ALGORITHM_QUERY_FACTORY);
            this.algorithmQueryName = this.databaseAccess.createQuery(ALGORITHM_NAME_QUERY_FACTORY);
            this.insertExperimentWriter = this.databaseAccess.createBatchWriter(INSERT_EXPERIMENT_WRITER_FACTORY);
            this.deleteExperimentWriter = this.databaseAccess.createBatchWriter(DELETE_EXPERIMENT_WRITER_FACTORY);
            this.experimentQuery = this.databaseAccess.createQuery(EXPERIMENT_QUERY_FACTORY);
            this.updateExperimentWriter = this.databaseAccess.createBatchWriter(UPDATE_EXPERIMENT_WRITER_FACTORY);
            this.insertParameterWriter = this.databaseAccess.createBatchWriter(INSERT_EXPERIMENT_PARAMETER_WRITER_FACTORY);
            this.deleteParameterWriter = this.databaseAccess.createBatchWriter(DELETE_EXPERIMENT_PARAMETER_WRITER_FACTORY);
            this.parameterQuery = this.databaseAccess.createQuery(PARAMETER_QUERY_FACTORY);
            this.insertAnnotationWriter = this.databaseAccess.createBatchWriter(INSERT_EXPERIMENT_ANNOTATION_WRITER_FACTORY);
            this.deleteAnnotationWriter = this.databaseAccess.createBatchWriter(DELETE_EXPERIMENT_ANNOTATION_WRITER_FACTORY);
            this.annotationQuery = this.databaseAccess.createQuery(ANNOTATION_QUERY_FACTORY);
            
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize writers.", e);
        }
    }

    
    
    /**
     * Loads all experiments for an algorithm.
     *
     * @param the algorithm whose experiments shall be loaded
     * @return the loaded experiments
     */
	public Collection<Experiment> getAllExperimentsForAlgorithm(
			RDBMSAlgorithm algorithm) {
        Collection<Experiment> allExperimentsForTable = allExperimentsForAlgorithmCache.get(algorithm);
        if (allExperimentsForTable != null) {
            return allExperimentsForTable;
        }
        try {
            Collection<Experiment> experiments = new HashSet<>();

            String sqlExperimentsForAlgorithm = String
                    .format("SELECT experiment.id as id from experiment where experiment.algorithmId = %d;",
                            algorithm.getId());

            ResultSet rs = databaseAccess.query(sqlExperimentsForAlgorithm, "experiment");
            while (rs.next()) {
                experiments.add(getExperimentById(rs.getInt("id")));
            }
            rs.close();
            allExperimentsForAlgorithmCache.put(algorithm, experiments);
            return allExperimentsForAlgorithmCache.get(algorithm);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}

	/**
	 * Retrieves an experiment by id if existent
	 * @param id of the experiment
	 * @return the experiment
	 */
	public Experiment getExperimentById(int experimentId) {
        Experiment cached = experimentCache.get(experimentId);
        if (cached != null) {
            return cached;
        }

        try (ResultSet rs = this.experimentQuery.execute(experimentId)) {
            while (rs.next()) {
            	experimentCache.put(experimentId, new
                        RDBMSExperiment(rs.getInt("id"),
                                rs.getString("description"),
                        		rs.getLong("executionTime"),
                                getAlgorithmFor(rs.getInt("algorithmId")),
                                getParametersFor(rs.getInt("id")),
                                getAnnotations(rs.getInt("id")),
                                rs.getString("timestamp"),
                                this.sqliteInterface));
                return experimentCache.get(experimentId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
		return null;
	}


	/**
	 * Retrieves an algorithm by id if existent
	 * @param id of the algorithm
	 * @return the algorithm
	 */
	public Algorithm getAlgorithmFor(int algorithmId) {
        Algorithm cached = algorithmCache.get(algorithmId);
        if (cached != null) {
            return cached;
        }

        try (ResultSet rs = this.algorithmQuery.execute(algorithmId)) {
            while (rs.next()) {
            	algorithmCache.put(algorithmId, new
                        RDBMSAlgorithm(rs.getInt("id"),
                                rs.getString("name"),
                                this.sqliteInterface));
                return algorithmCache.get(algorithmId);
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}

	/**
	 * Retrieves the annotations of an experiment
	 * @param id of the experiment
	 * @return set of annotations
	 */
	private Set<Annotation> getAnnotations(int experimentId) {
        Experiment cached = experimentCache.get(experimentId);
        if (cached != null) {
            return new HashSet<Annotation>(cached.getAnnotations());
        }

        try (ResultSet rs = this.annotationQuery.execute(experimentId)) {
        	Set<Annotation> annotations = new HashSet<Annotation>(); 
            while (rs.next()) {
            	annotations.add(new Annotation(rs.getString("tag"), rs.getString("textt")));
            }
             return annotations;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}

/**
 * Retrieves parameters of an experiment
 * @param id of the experiment
 * @return map containing the parameter settings
 */
	private Map<String, String> getParametersFor(int experimentId) {
        Experiment cached = experimentCache.get(experimentId);
        if (cached != null) {
            return cached.getParameters();
        }

        try (ResultSet rs = this.parameterQuery.execute(experimentId)) {
        	Map<String, String> parameters = new HashMap<String, String>(); 
            while (rs.next()) {
            	parameters.put(rs.getString("keyy"), rs.getString("value"));
            }
             return parameters;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}



	public void addAlgorithm(RDBMSAlgorithm algorithm) {
		try {
			this.insertAlgorithmWriter.write(algorithm);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		};
	}

	public void writeExperiment(RDBMSExperiment experiment) {
		try {
			this.insertExperimentWriter.write(experiment);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		};
		for (String key : experiment.getParameters().keySet()){
			addParameterToExperiment(experiment, key, experiment.getParameters().get(key));
		}
		
		for (Annotation annotation : experiment.getAnnotations()){
			addAnnotation(experiment, annotation.getTag(), annotation.getText());
		}
		
	}

	public void addParameterToExperiment(Experiment experiment, String key,
			String value) {
		Object[] parameters = new Object[3];
		parameters[0] = experiment.getId();
		parameters[1] = key;
		parameters[2] = value;
		experimentCache.remove(experiment.getId());
		
		try{
			this.insertParameterWriter.write(parameters);			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void addAnnotation(RDBMSExperiment experiment,
			String tag, String text) {
		Object[] parameters = new Object[3];
		parameters[0] = experiment.getId();
		parameters[1] = tag;
		parameters[2] = text;
		experimentCache.remove(experiment.getId());
		try{
			this.insertAnnotationWriter.write(parameters);			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void setExecutionTimeToExperiment(Experiment experiment,
			long executionTime) {
		Object[] parameters = new Object[2];
		parameters[0] = experiment.getId();
		parameters[1] = executionTime;
		
		experimentCache.remove(experiment.getId());
		
		try{
			this.updateExperimentWriter.write(parameters);			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Collection<Algorithm> getAllAlgorithms() {
        try {
            Collection<Algorithm> algorithms = new HashSet<>();

            String sqlAlgorithms = "SELECT algorithm.id as id, algorithm.name as name from algorithm;";

            ResultSet rs = databaseAccess.query(sqlAlgorithms, "algorithm");
            while (rs.next()) {
                algorithms.add(new RDBMSAlgorithm(rs.getInt("id"), rs.getString("name"), this.sqliteInterface));
            }
            rs.close();
            return algorithms;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}

	public Collection<Experiment> getAllExperiments() {
        try {
            Collection<Experiment> experiments = new HashSet<>();

            String sqlExperiments = "SELECT experiment.id as id, experiment.description as description,"
            		+ " experiment.algorithmId as algorithmId, experiment.timestamp as timestamp, experiment.executionTime as executionTime from experiment ;";

            ResultSet rs = databaseAccess.query(sqlExperiments, "experiment");
            while (rs.next()) {
                experiments.add(new
                        RDBMSExperiment(rs.getInt("id"),
                                rs.getString("description"),
                        		rs.getLong("executionTime"),
                                getAlgorithmFor(rs.getInt("algorithmId")),
                                getParametersFor(rs.getInt("id")),
                                getAnnotations(rs.getInt("id")),
                                rs.getString("timestamp"),
                                this.sqliteInterface));
            }
            rs.close();
            return experiments;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}
	
	public void removeAlgorithm(Algorithm algorithm) {
		try {
			this.deleteAlgorithmWriter.write(algorithm);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void removeExperiment(Experiment experiment) {
		try{
			this.deleteExperimentWriter.write(experiment);
			this.deleteAnnotationWriter.write(experiment.getId());
			this.deleteParameterWriter.write(experiment.getId());
		}catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	public Algorithm getAlgorithmByName(String name) {
		for (Algorithm cached : algorithmCache.values()) {
			if (cached.getName().equals(name)){
				return cached;
			}
		}
        try (ResultSet rs = this.algorithmQueryName.execute(name)) {
            while (rs.next()) {
            	algorithmCache.put(rs.getInt("id"), new
                        RDBMSAlgorithm(rs.getInt("id"),
                                rs.getString("name"),
                                this.sqliteInterface));
                return algorithmCache.get(rs.getInt("id"));
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}
	
}
