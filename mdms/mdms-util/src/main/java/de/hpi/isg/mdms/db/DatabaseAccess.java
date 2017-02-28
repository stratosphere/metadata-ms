package de.hpi.isg.mdms.db;

import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.SQLQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery.Factory;
import de.hpi.isg.mdms.db.write.BatchWriter;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.DependentWriter;
import de.hpi.isg.mdms.db.write.SQLExecutor;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Manages the access to a database by maintaining batch writers and ensuring all data is written before performing a
 * read.
 *
 * @author Sebastian Kruse
 */
public class DatabaseAccess implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseAccess.class);

    /**
     * The connection to the database.
     */
    private final Connection connection;

    /**
     * A set of writers that operate on specific tables, usually {@link BatchWriter}s.
     */
    private Map<String, Set<DependentWriter<?>>> manipulatingWriters = new HashMap<>();

    /**
     * A set of writers that operate on specific tables, usually {@link BatchWriter}s.
     */
    private Map<String, Set<DependentWriter<?>>> accessingWriters = new HashMap<>();

    /**
     * Captures for each writer, which writer have to flushed before flushing this writer.
     */
    private Map<DependentWriter<?>, Set<DependentWriter<?>>> preceedingWriters = new HashMap<>();

    /**
     * Executes plain SQL INSERT/UPDATE statements.
     */
    private SQLExecutor sqlExecutor;

    /**
     * (Probably prepared) queries that are available in the database access.
     */
    private Collection<DatabaseQuery<?>> queries = new LinkedList<>();

    /**
     * Executes plain SQL SELECT statements.
     */
    private SQLQuery sqlQuery;

    /**
     * A mapping from tables to referenced tables (via foreign keys).
     */
    private Map<String, Set<String>> foreignKeyDependencies = new HashMap<>();

    public DatabaseAccess(Connection connection) {
        super();
        try {
            Validate.isTrue(!connection.isClosed());
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.connection = connection;
        try {
            this.sqlExecutor = new SQLExecutor(this, BatchWriter.DEFAULT_BATCH_SIZE);
            this.sqlQuery = new SQLQuery(this);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads the foreign keys from the RDBMS.
     */
    public Set<String> getReferencedTables(String table) {
        table = canonicalizeTableName(table);
        Set<String> referencedTables = this.foreignKeyDependencies.get(table);
        if (referencedTables != null) {
            return referencedTables;
        }
        try {
            referencedTables = new HashSet<>();
            DatabaseMetaData metaData = this.connection.getMetaData();
            ResultSet resultSet = metaData.getImportedKeys(null, null, table);
            while (resultSet.next()) {
                String referencedTable = canonicalizeTableName(resultSet.getString("PKTABLE_NAME"));
                referencedTables.add(referencedTable);
            }
            resultSet.close();
            this.foreignKeyDependencies.put(table, referencedTables);
            return referencedTables;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <TWriter extends BatchWriter<TData>, TData> TWriter createBatchWriter(DatabaseWriter.Factory<TWriter> factory)
            throws SQLException {

        TWriter writer = factory.createWriter(this);
        notifyAccess(writer, writer.getAccessedTables());
        notifyManipulation(writer, writer.getManipulatedTables());

        return writer;
    }

    /**
     * Executes a SQL statement on the managed database. Thereby, dependencies to other query batches are respected.
     *
     * @param sqlStmt          is the SQL statement to execute
     * @param manipulatedTable is the table that is manipulated by this query
     * @param queriedTables    are the affecting/affected referenced tables of this query. If no referenced tables are passed, they
     *                         are deduced from the foreign key relationships of the database.
     * @throws java.sql.SQLException
     */
    public void executeSQL(String sqlStmt, String manipulatedTable, String... queriedTables)
            throws SQLException {

        manipulatedTable = canonicalizeTableName(manipulatedTable);
        queriedTables = canonicalizeTableNames(queriedTables);

        List<String> accessedTables = new ArrayList<>();
        for (String queriedTable : queriedTables) {
            accessedTables.add(queriedTable);
        }
        Set<String> referencedTables = getReferencedTables(manipulatedTable);
        accessedTables.addAll(referencedTables);
        this.sqlExecutor.write(sqlStmt, new String[]{manipulatedTable},
                accessedTables.toArray(new String[accessedTables.size()]));
    }

    private String canonicalizeTableName(String tableName) {
        if (tableName == null) {
            return null;
        }
        return tableName.toLowerCase();
    }

    private String[] canonicalizeTableNames(String[] tableNames) {
        for (int i = 0; i < tableNames.length; i++) {
            tableNames[i] = canonicalizeTableName(tableNames[i]);
        }
        return tableNames;
    }

    // /**
    // * Executes a SQL statement on the managed database. Thereby, dependencies to other query batches are respected.
    // *
    // * @param sqlStmt
    // * is the SQL statement to execute
    // * @param manipulatedTables
    // * are the tables that are manipulated by this query
    // * @throws SQLException
    // */
    // public void executeSQL(String sqlStmt, String... manipulatedTables)
    // throws SQLException {
    //
    // // TODO: the flush order might not be well-suited for deletions. Handle that differently?
    // String[] referencedTables = new String[0];
    // for (String manipulatedTable : manipulatedTables) {
    // referencedTables = updateReferencedTablesIfNotGiven(manipulatedTable, referencedTables);
    //
    // }
    // this.sqlExecutor.write(sqlStmt, manipulatedTables, referencedTables);
    // }
//
//	private String[] updateReferencedTablesIfNotGiven(String manipulatedTable, String... referencedTables) {
//		if (referencedTables.length == 0) {
//			Set<String> fkReferencedTables = getReferencedTables(manipulatedTable);
//			referencedTables = fkReferencedTables.toArray(new String[fkReferencedTables.size()]);
//		} else {
//			Logger.getGlobal().warning(String.format("Manually passed referenced tables detected: %s: %s.",
//					manipulatedTable, Arrays.toString(referencedTables)));
//		}
//		return referencedTables;
//	}

    public ResultSet query(String sql, String... queriedTables) throws SQLException {
        queriedTables = canonicalizeTableNames(queriedTables);
        flush(Arrays.asList(queriedTables));
        return this.sqlQuery.execute(sql, queriedTables);
    }

    /**
     * Flushes all writers.
     *
     * @throws java.sql.SQLException if the flushing fails for any writer.
     */
    public void flush() throws SQLException {
        int lastNumWriters;
        while ((lastNumWriters = this.manipulatingWriters.size()) > 0) {
            DependentWriter<?> anyActiveWriter = this.manipulatingWriters.values().iterator().next().iterator().next();
            anyActiveWriter.flush();
            if (lastNumWriters == this.manipulatingWriters.size()) {
                LOGGER.error("Flushing {} was without effect.", anyActiveWriter);
                LOGGER.warn("Skip flushing {}...", anyActiveWriter);
                for (Iterator<Map.Entry<String, Set<DependentWriter<?>>>> iterator = this.manipulatingWriters.entrySet().iterator();
                     iterator.hasNext(); ) {
                    Map.Entry<String, Set<DependentWriter<?>>> entry = iterator.next();
                    entry.getValue().remove(anyActiveWriter);
                    if (entry.getValue().isEmpty()) iterator.remove();
                }
            }
        }
    }

    /**
     * Flushes any writer that has to preceed the given writer. Furthermore, any registered access on database tables of
     * this writer will be removed.
     *
     * @param writerToFlush is the writer that is about to be flushed
     * @throws java.sql.SQLException
     */
    public void prepareFlush(DependentWriter<?> writerToFlush) throws SQLException {

        Set<DependentWriter<?>> preceedingWriters = this.preceedingWriters.get(writerToFlush);
        if (preceedingWriters != null) {
            for (DependentWriter<?> preceedingWriter : preceedingWriters) {
                preceedingWriter.flush();
            }
            this.preceedingWriters.remove(writerToFlush);
        }

        for (String accessedTable : writerToFlush.getAccessedTables()) {
            accessedTable = canonicalizeTableName(accessedTable);
            Set<DependentWriter<?>> accessingWriters = this.accessingWriters.get(accessedTable);
            if (accessingWriters.size() > 1) {
                accessingWriters.remove(writerToFlush);
            } else {
                this.accessingWriters.remove(accessedTable);
            }
        }
        for (String manipulatedTable : writerToFlush.getManipulatedTables()) {
            manipulatedTable = canonicalizeTableName(manipulatedTable);
            Set<DependentWriter<?>> manipulatingWriters = this.manipulatingWriters.get(manipulatedTable);
            if (manipulatingWriters.size() > 1) {
                manipulatingWriters.remove(writerToFlush);
            } else {
                this.manipulatingWriters.remove(manipulatedTable);
            }
        }
    }

    /**
     * Flushes (at least) all writers that operate on the given tables.
     *
     * @param accessedTables are the tables for which writers shall be flushed.
     * @throws java.sql.SQLException if the flushing fails for any of the writers.
     */
    public void flush(Collection<String> tables) throws SQLException {
        for (String table : tables) {
            table = canonicalizeTableName(table);
            Collection<DependentWriter<?>> writers = this.manipulatingWriters.get(table);
            if (writers == null) {
                continue;
            }
            LOGGER.debug("Flushing modifications on {}...", table);
            for (DependentWriter<?> writer : new ArrayList<>(writers)) {
                writer.flush();
            }
            LOGGER.debug("...done flushing! (on {})", table);
        }
    }

    /**
     * Flushes any pending changes and closes the connection.
     *
     * @throws java.sql.SQLException if the flushing or closing fails
     */
    public void close() throws SQLException {
        try {
            flush();
        } finally {
            tryToClose(this.sqlExecutor);
            tryToClose(this.sqlQuery);
            for (Set<DependentWriter<?>> writers : this.accessingWriters.values()) {
                for (DependentWriter<?> writer : writers) {
                    tryToClose(writer);
                }
            }
            accessingWriters.clear();
            for (DatabaseQuery<?> query : this.queries) {
                tryToClose(query);
            }
            this.queries.clear();
            this.connection.close();
        }
    }

    /**
     * Closes an {@link AutoCloseable} if not {@code null} and catches any exceptions.
     */
    private void tryToClose(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return the database connection that is managed by this object.
     */
    public Connection getConnection() {
        return this.connection;
    }

    public <TElement> DatabaseQuery<TElement> createQuery(Factory<TElement> locationQueryFactory) {
        return locationQueryFactory.createQuery(this);
    }

    public void notifyWriterAction(DependentWriter<?> writer, Collection<String> manipulatedTables,
                                   Collection<String> accessedTables) {

        if (accessedTables.isEmpty() && manipulatedTables.isEmpty()) {
            return;
        }

        // Step 1: Find which old operations must be done before this access.
        Set<DependentWriter<?>> newPreceedingWriters = new HashSet<>();
        Set<DependentWriter<?>> knownPreceedingWriters = this.preceedingWriters.get(writer);
        for (String accessedTable : accessedTables) {
            // Find out if this is a new access.
            accessedTable = canonicalizeTableName(accessedTable);
            Set<DependentWriter<?>> accessingWriters = this.accessingWriters.get(accessedTable);
            if (accessingWriters == null || accessingWriters.contains(writer)) {
                continue;
            }

            // Find out if any writer manipulates that field.
            Set<DependentWriter<?>> manipulatingWriters = this.manipulatingWriters.get(accessedTable);
            if (manipulatingWriters == null) {
                continue;
            }
            for (DependentWriter<?> manipulatingWriter : manipulatingWriters) {
                if (knownPreceedingWriters == null || !knownPreceedingWriters.contains(manipulatingWriter)) {
                    newPreceedingWriters.add(manipulatingWriter);
                }
            }
        }

        // Step 2: See if new preceeding writers were already preceeded by this writer.
        for (DependentWriter<?> newPreceedingWriter : newPreceedingWriters) {
            // Reformulation: We have a new edge stating "manipulatingWriter preceeds writer".
            // As the graph was acyclic, new cycles must involve the new edge.
            // Thus, we want to find any path stating "writer preceeds manipulatingWriter".
            Collection<List<DependentWriter<?>>> predecessorPaths = searchIsPreceededPaths(newPreceedingWriter, writer);

            // If there are cycles, we need to break those up. The lowest-impact solution would be
            // to flush the writer that preceeds the other writers anyway, i.e., is preceeded by this writer
            // i.e., the second-to-last writer in any circle.
            if (!predecessorPaths.isEmpty()) {
                for (List<DependentWriter<?>> predecessorPath : predecessorPaths) {
                    LOGGER.debug("Cycle detected: {}.", predecessorPath);
                    DependentWriter<?> writerToFlush = predecessorPath.get(predecessorPath.size() - 2);
                    try {
                        writerToFlush.flush();
                        ;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        notifyManipulation(writer, manipulatedTables);
        notifyAccess(writer, accessedTables);
    }

    /**
     * Tell that a writer received a request to manipulate a certain table.
     *
     * @param writer            is the writer that manipulates the table
     * @param manipulatedTables are the tables to manipulate
     */
    private void notifyManipulation(DependentWriter<?> writer, Collection<String> manipulatedTables) {
        if (manipulatedTables.isEmpty()) {
            return;
        }

        for (String manipulatedTable : manipulatedTables) {
            manipulatedTable = canonicalizeTableName(manipulatedTable);
            // In general, we assume that a manipulation does not effect accesses, which is of course not always true.
            // Therefore, we still need to consider SQL interdependencies in the code.

            // Thus, it suffices to mark the manipulation of this writer.
            Set<DependentWriter<?>> adjacenceSet = this.manipulatingWriters.get(manipulatedTable);
            if (adjacenceSet == null) {
                adjacenceSet = new HashSet<>();
                this.manipulatingWriters.put(manipulatedTable, adjacenceSet);
            }
            // Verify that we actually add this writer, i.e., that we have a new manipulated table.
            if (adjacenceSet.add(writer)) {
                LOGGER.trace("Manipulation: {} by {}", manipulatedTable, writer);
            }
            // In this case, we have new "edges" in the data-dependency graph and need to check for cycles.
            // Check for cycles starting with these edges.
        }
    }

    /**
     * Tell that a writer wants to access a table.
     *
     * @param writer         is the writer that wants to perform the access
     * @param accessedTables are the tables to be accessed
     */
    // TODO
    private void notifyAccess(DependentWriter<?> writer, Collection<String> accessedTables) {
        if (accessedTables.isEmpty()) {
            return;
        }

        Set<DependentWriter<?>> preceedingWriters = null;
        for (String accessedTable : accessedTables) {
            accessedTable = canonicalizeTableName(accessedTable);
            // Find out if this is a new access.
            Set<DependentWriter<?>> adjacenceSet = this.accessingWriters.get(accessedTable);
            if (adjacenceSet == null) {
                adjacenceSet = new HashSet<>();
                this.accessingWriters.put(accessedTable, adjacenceSet);
            }
            if (adjacenceSet.add(writer)) {
                LOGGER.trace("Access: {} by {}", accessedTables, writer);
            }

            // In this case, we have new "edges" in the data-dependency graph.
            Set<DependentWriter<?>> manipulatingWriters = this.manipulatingWriters.get(accessedTable);
            if (manipulatingWriters != null) {
                for (DependentWriter<?> manipulatingWriter : manipulatingWriters) {
                    // Reflexive manipulation relationships are allowed, since batches maintain the order of their
                    // queries.
                    // Hence, we do not add them in the first place to the flush-order graph.
                    if (manipulatingWriter == writer) {
                        continue;
                    }
                    // Lazy-initialize preceeding writers.
                    if (preceedingWriters == null) {
                        preceedingWriters = this.preceedingWriters.get(writer);
                        if (preceedingWriters == null) {
                            preceedingWriters = new HashSet<>();
                            this.preceedingWriters.put(writer, preceedingWriters);
                        }
                    }
                    if (preceedingWriters.add(manipulatingWriter)) {
                        LOGGER.trace("Preceed: {} must preceed {}.", manipulatingWriter, writer);
                    }
                }
            }
        }
    }

    /**
     * Searches any path between the given writers, following the relationship "a is predecessor of b".
     *
     * @param startWriter  is the start point of the search
     * @param targetWriter is the end point of the search
     * @return all shortest paths
     */
    private Collection<List<DependentWriter<?>>> searchIsPreceededPaths(DependentWriter<?> startWriter, DependentWriter<?> targetWriter) {
        Collection<List<DependentWriter<?>>> paths = new LinkedList<>();
        LinkedList<DependentWriter<?>> visitedNodes = new LinkedList<>();
        searchPaths(startWriter, targetWriter, visitedNodes, paths);
        return paths;
    }

    /**
     * Recursively executed auxillary method to search paths between two nodes.
     *
     * @param startWriter  is the currently investigated node to expand
     * @param targetWriter is the target node of the search
     * @param visitedNodes is the path that has been taken until the currently explored node
     * @param paths        collects newly found paths to the target node
     */
    private void searchPaths(DependentWriter<?> startWriter, DependentWriter<?> targetWriter,
                             LinkedList<DependentWriter<?>> visitedNodes,
                             Collection<List<DependentWriter<?>>> paths) {

        Set<DependentWriter<?>> preceedingWriters = this.preceedingWriters.get(startWriter);
        if (preceedingWriters != null) {
            visitedNodes.add(startWriter);
            for (DependentWriter<?> preceedingWriter : preceedingWriters) {
                if (preceedingWriter == targetWriter) {
                    visitedNodes.add(preceedingWriter);
                    paths.add(new ArrayList<>(visitedNodes));
                    visitedNodes.removeLast();
                } else if (!visitedNodes.contains(preceedingWriter)) {
                    searchPaths(preceedingWriter, targetWriter, visitedNodes, paths);
                }
            }
            visitedNodes.removeLast();
        }

    }
//  TODO Delete code snippet.
//	public void notifyTablesClear(DependentWriter<?> writer) {
//		LOGGER.trace("Clear manipuations and accesses {}.", writer);
//	}

}
