package de.hpi.isg.metadata_store.db.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import de.hpi.isg.metadata_store.db.DatabaseAccess;


abstract public class PreparedQuery<T> extends DatabaseQuery<T> {

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
		this.databaseAccess.flush(this.queriedTables);
		ensureStatementPrepared();
		setStatementParameters(element);
		return this.preparedStatement.executeQuery();
	}

	abstract protected void setStatementParameters(T element) throws SQLException;

	public void close() throws SQLException {
		if (this.preparedStatement != null) {
			this.preparedStatement.close();
		}
	}
	
}
