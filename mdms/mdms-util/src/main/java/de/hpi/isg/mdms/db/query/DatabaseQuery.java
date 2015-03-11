package de.hpi.isg.mdms.db.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import de.hpi.isg.mdms.db.DatabaseAccess;

/**
 * Database queries are to be used in connection with a {@link DatabaseAccess} and serve several purposes:
 * <ol>
 * <li>share database resources among queries</li>
 * <li>coordinate queries with pending writes</li>
 * <li>save query preparation overhead</li>
 * </ol>
 * 
 * @author Sebastian Kruse
 * 
 * @param <T>
 */
abstract public class DatabaseQuery<T> implements AutoCloseable {

	protected final DatabaseAccess databaseAccess;

	public DatabaseQuery(DatabaseAccess databaseAccess) {
		super();
		this.databaseAccess = databaseAccess;
	}

	/**
	 * Executes this query and returns the appropriate result set.
	 * 
	 * @param queryParameter
	 *            is an object that defines the parameters of the query
	 * @return the result set of the query
	 * @throws java.sql.SQLException
	 *             if the query could not be executed properly
	 */
	abstract public ResultSet execute(T queryParameter) throws SQLException;
	
	/**
	 * Closes the database resources held by this query.
	 * 
	 * @throws java.sql.SQLException if the closing fails
	 */
	abstract public void close() throws SQLException;

	public static interface Factory<TQuery extends DatabaseQuery<?>> {

		TQuery createQuery(DatabaseAccess databaseAccess);

	}
}
