package de.hpi.isg.metadata_store.db.write;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * This {@link DatabaseWriter} can be used to execute generic SQL statements.
 * 
 * @author Sebastian Kruse
 * 
 */
public class SQLExecutor extends BatchWriter<String> {

    /** Helper set to keep track of incoming manipulated tables. */
    private final Set<String> newManipulatedTables = new HashSet<>();

    /** Helper set to keep track of incoming acessed tables. */
    private final Set<String> newAccessedTables = new HashSet<>();

    public SQLExecutor(DatabaseAccess databaseAccess,
            int batchSize) throws SQLException {

        super(databaseAccess,
                Collections.<String> emptySet(),
                Collections.<String> emptySet(),
                batchSize);
    }

    public void write(String element, String[] manipulatedTables, String... referencedTables) throws SQLException {
        // Keep track of manipulated tables.
        for (String manipulatedTable : manipulatedTables) {
            this.manipulatedTables.add(manipulatedTable);
            this.newManipulatedTables.add(manipulatedTable);
        }
        
        // Keep track of accessed tables.
        for (String referencedTable : referencedTables) {
            this.accessedTables.add(referencedTable);
            this.newAccessedTables.add(referencedTable);
        }
        
        // Do the write.
        super.write(element);
    }

    @Override
    public void write(String element) throws SQLException {
        throw new UnsupportedOperationException("Used #write(String, String, String...) instead.");
    }

    @Override
    protected void ensureStatementInitialized() throws SQLException {
        if (this.statement == null) {
            this.statement = this.connection.createStatement();
        }
    }

    @Override
    protected void addBatch(String sql) throws SQLException {
        this.statement.addBatch(sql);
    }

    @Override
    protected void doFlush() throws SQLException {
        super.doFlush();
        this.manipulatedTables.clear();
        this.accessedTables.clear();
        // TODO Delete code.
//        this.databaseAccess.notifyTablesClear(this);
    }
    
    @Override
    protected void fireAboutToAddBatchElement() {
        if (!this.newManipulatedTables.isEmpty() || !this.newAccessedTables.isEmpty()) {
        	this.databaseAccess.notifyWriterAction(this, this.newManipulatedTables, this.newAccessedTables);
        	this.newManipulatedTables.clear();
        	this.newAccessedTables.clear();
        }
    }

    @Override
    public String toString() {
        return "SQLExecutor [manipulatedTables=" + getManipulatedTables() + ", accessedTables=" + getAccessedTables() + "]";
    }

}
