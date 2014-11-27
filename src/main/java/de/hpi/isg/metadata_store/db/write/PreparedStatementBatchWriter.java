package de.hpi.isg.metadata_store.db.write;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import de.hpi.isg.metadata_store.db.DatabaseAccess;
import de.hpi.isg.metadata_store.db.PreparedStatementAdapter;

public class PreparedStatementBatchWriter<T> extends BatchWriter<T> {

    /**
     * Stores elements as batches to the prepared statement.
     */
    private final PreparedStatementAdapter<T> adapter;
	
    /**
     * The {@link PreparedStatement}-style SQL that shall be executed by this writer.
     */
    private final String preparedSql;

    public PreparedStatementBatchWriter(DatabaseAccess databaseAccess, String preparedSql,
            Collection<String> manipulatedTables, int batchSize, PreparedStatementAdapter<T> adapter) {

        super(databaseAccess, manipulatedTables, batchSize);
        this.preparedSql = preparedSql;
        this.adapter = adapter;
    }
    
    @Override
    protected void ensureStatementInitialized() throws SQLException {
    	if (this.statement == null) {
    		this.statement = this.connection.prepareStatement(this.preparedSql);
    	}
    }

    @Override
    protected void addBatch(T element) throws SQLException {
        this.adapter.translateParameter(element, (PreparedStatement) this.statement);
        ((PreparedStatement) this.statement).addBatch();
    }

    public static class Factory<TElement> implements DatabaseWriter.Factory<PreparedStatementBatchWriter<TElement>> {

        private final String sqlStatement;

        private final PreparedStatementAdapter<TElement> adapter;

        private Collection<String> manipulatedTables;

        public Factory(String sqlStatement, PreparedStatementAdapter<TElement> adapter, String manipulatedTable) {
            this.sqlStatement = sqlStatement;
            this.adapter = adapter;
            this.manipulatedTables = Collections.singleton(manipulatedTable);
        }

        @Override
        public PreparedStatementBatchWriter<TElement> createWriter(DatabaseAccess databaseAccess) throws SQLException {

            return new PreparedStatementBatchWriter<TElement>(databaseAccess,
                    this.sqlStatement, this.manipulatedTables,
                    DEFAULT_BATCH_SIZE, this.adapter);
        }

    }
}
