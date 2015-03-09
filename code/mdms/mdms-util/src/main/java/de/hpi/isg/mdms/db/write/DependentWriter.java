package de.hpi.isg.mdms.db.write;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.mdms.db.DatabaseAccess;

/**
 * A {@link de.hpi.isg.mdms.db.write.DependentWriter} inserts/updates tuples that reference tuples of other tables. A {@link DatabaseAccess}
 * object can manage these dependencies. The {@link de.hpi.isg.mdms.db.write.DependentWriter} offers utility methods to manage its own
 * dependencies and let the {@link DatabaseAccess} take care of enforcing referential integrity.
 * 
 * @author Sebastian Kruse
 * 
 */
abstract public class DependentWriter<T> extends DatabaseWriter<T> {

    private static Logger LOGGER = LoggerFactory.getLogger(DatabaseWriter.class);
    
	/**
	 * A {@link DatabaseAccess} that manages dependencies among writers.
	 */
	protected final DatabaseAccess databaseAccess;

	/**
	 * The names of the tables that are referenced by the manipulated tables.
	 */
	protected Set<String> accessedTables;
	
	/**
	 * Tells if the referenced tables of the manipulated tables have already been determined
	 * and added to the accessed tables.
	 */
	private boolean isReferencedTablesDetermined;

	/**
	 * The names of the tables that are manipulated by this writer or current batch.
	 */
	protected Set<String> manipulatedTables;
	
	public static Collection<String> findAllReferencedTables(Collection<String> manipulatedTables, DatabaseAccess databaseAccess) {
	    Collection<String> allReferencedTables = new LinkedList<>();
	    for (String manipulatedTable : manipulatedTables) {
	        Set<String> referencedTables = databaseAccess.getReferencedTables(manipulatedTable);
	        allReferencedTables.addAll(referencedTables);
	    }
	    return allReferencedTables;
	}
	

	public DependentWriter(DatabaseAccess databaseAccess, Collection<String> manipulatedTables) {
        this(databaseAccess, Collections.<String> emptySet(), manipulatedTables);
	    
	}
	
	public DependentWriter(DatabaseAccess databaseAccess, Collection<String> accessedTables, Collection<String> manipulatedTables) {
		super(databaseAccess.getConnection());

		this.databaseAccess = databaseAccess;
		this.accessedTables = new HashSet<>(accessedTables);
		this.isReferencedTablesDetermined = false;
		this.manipulatedTables = new HashSet<>(manipulatedTables);
	}
	
	protected void ensureReferencedTablesDetermined() {
		if (!this.isReferencedTablesDetermined) {
			this.accessedTables.addAll(findAllReferencedTables(manipulatedTables, databaseAccess));
			this.isReferencedTablesDetermined = true;
		}
	}
	
//	protected void ensureReferencedTablesFlushed() throws SQLException {
//		if (!this.accessedTables.isEmpty()) {
//			this.databaseAccess.flush(this.accessedTables);
//		}
//	}

	public Set<String> getManipulatedTables() {
		return manipulatedTables;
	}
	
	public Set<String> getAccessedTables() {
		return accessedTables;
	}


	@Override
    public void flush() throws SQLException {
        if (this.statement != null) {
            // Logger.getGlobal().log(Level.INFO, String.format("Flushing %s.", this));
            this.databaseAccess.prepareFlush(this);
            try {
                doFlush();
            } catch (SQLException e) {
                LOGGER.error("{} when flushing {}.", e.getClass().getSimpleName(), this);
                throw e;
            }
        }
    }


    protected abstract void doFlush() throws SQLException;
}
