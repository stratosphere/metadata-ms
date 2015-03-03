package de.hpi.isg.mdms.db.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.mdms.db.DatabaseAccess;

public class SQLQuery extends DatabaseQuery<SQLQuery.Parameters> {

    private static final Logger LOGGER =  LoggerFactory.getLogger(SQLQuery.class);
    
	/**
	 * Statement over which queries can be posed. This is to be lazy-initialized.
	 */
	private Statement statement;

	public SQLQuery(DatabaseAccess databaseAccess) {
		super(databaseAccess);
	}

	/**
	 * Convenience method for {@link #execute(Parameters)}.
	 */
	public ResultSet execute(String sql, String... queriedTables) throws SQLException {
		return execute(new SQLQuery.Parameters(sql, queriedTables));
	}

	/**
	 * Runs a query on the database. Ensures that relevant pending insert/update batches are executed first.
	 */
	@Override
	public ResultSet execute(SQLQuery.Parameters queryParameter) throws SQLException {
	    LOGGER.trace("Query issued: {}", queryParameter.sql);
		this.databaseAccess.flush(queryParameter.queriedTables);
		ensureStatementCreated();
        long startTime = System.currentTimeMillis();
        ResultSet resultSet = this.statement.executeQuery(queryParameter.sql);
        long endTime = System.currentTimeMillis();
        LOGGER.trace("ResultSet available after {} ms", (endTime - startTime));
        return resultSet;	}

	private void ensureStatementCreated() throws SQLException {
		if (this.statement == null) {
			Connection connection = this.databaseAccess.getConnection();
			this.statement = connection.createStatement();
		}
	}

	@Override
	public void close() throws SQLException {
		if (this.statement != null) {
			this.statement.close();
		}
	}
	
	/**
	 * Parameters define the necessary input for a {@link SQLQuery}.
	 * 
	 * @author Sebastian Kruse
	 * 
	 */
	public static class Parameters {

		private String sql;

		private Collection<String> queriedTables;

		/**
		 * Create new query parameters for a {@link SQLQuery}.
		 * 
		 * @param sql
		 *            is a SQL query to execute
		 * @param queriedTables
		 *            are the tables that are queried by the SQL query
		 */
		public Parameters(String sql, String... queriedTables) {
			this(sql, Arrays.asList(queriedTables));
		}

		/**
		 * Create new query parameters for a {@link SQLQuery}.
		 * 
		 * @param sql
		 *            is a SQL query to execute
		 * @param queriedTables
		 *            are the tables that are queried by the SQL query
		 */
		public Parameters(String sql, Collection<String> queriedTables) {
			this.sql = sql;
			this.queriedTables = queriedTables;
		}

	}

}
