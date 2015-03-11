package de.hpi.isg.metadata_store.db.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.db.DatabaseAccess;


abstract public class PreparedQuery<T> extends DatabaseQuery<T> {
    
    private static final Logger LOGGER =  LoggerFactory.getLogger(PreparedQuery.class);

	private final String sql;
	
	protected PreparedStatement preparedStatement;

	private List<String> queriedTables;
	
	public PreparedQuery(DatabaseAccess databaseAccess, String sql, String... queriedTables) {
		super(databaseAccess);
		this.sql = sql;
		this.queriedTables = Arrays.asList(queriedTables);
	}
	
	private void ensureStatementPrepared() throws SQLException {
		if (this.preparedStatement == null) {
			this.preparedStatement = this.databaseAccess.getConnection().prepareStatement(this.sql);
		}
	}
	
	@Override
	public ResultSet execute(T element) throws SQLException {
	    LOGGER.trace("Query issued: {} with {}", this.sql, element);
		this.databaseAccess.flush(this.queriedTables);
		ensureStatementPrepared();
		setStatementParameters(element);
		long startTime = System.currentTimeMillis();
		ResultSet resultSet = this.preparedStatement.executeQuery();
		long endTime = System.currentTimeMillis();
		LOGGER.trace("ResultSet available after {} ms", (endTime - startTime));
        return resultSet;
	}

	abstract protected void setStatementParameters(T element) throws SQLException;

	@Override
	public void close() throws SQLException {
		if (this.preparedStatement != null) {
			this.preparedStatement.close();
		}
	}
	
}
