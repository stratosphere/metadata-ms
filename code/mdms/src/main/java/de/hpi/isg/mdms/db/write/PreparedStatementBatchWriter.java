package de.hpi.isg.mdms.db.write;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;

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
            Collection<String> accessedTables, Collection<String> manipulatedTables, 
            int batchSize, PreparedStatementAdapter<T> adapter) {

        super(databaseAccess, accessedTables, manipulatedTables, batchSize);
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

    
    
    @Override
    public String toString() {
        return "PreparedStatementBatchWriter [" + preparedSql + "]";
    }



    public static class Factory<TElement> implements DatabaseWriter.Factory<PreparedStatementBatchWriter<TElement>> {

        private final String sqlStatement;

        private final PreparedStatementAdapter<TElement> adapter;

        private Collection<String> manipulatedTables;

        private Collection<String> accessedTables;

        public Factory(String sqlStatement, PreparedStatementAdapter<TElement> adapter, String manipulatedTable, String... accessedTables) {
            this.sqlStatement = sqlStatement;
            this.adapter = adapter;
            this.manipulatedTables = Collections.singleton(manipulatedTable);
            this.accessedTables = Arrays.asList(accessedTables);
        }
        

        @Override
        public PreparedStatementBatchWriter<TElement> createWriter(DatabaseAccess databaseAccess) throws SQLException {

            return new PreparedStatementBatchWriter<TElement>(databaseAccess,
                    this.sqlStatement, this.accessedTables, this.manipulatedTables,
                    DEFAULT_BATCH_SIZE, this.adapter);
        }

    }
}
