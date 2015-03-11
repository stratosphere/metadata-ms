package de.hpi.isg.metadata_store.rdbms;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.isg.metadata_store.db.DatabaseAccess;
import de.hpi.isg.metadata_store.db.PreparedStatementAdapter;
import de.hpi.isg.metadata_store.db.write.DatabaseWriter;
import de.hpi.isg.metadata_store.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.factories.SQLiteInterface;
import de.hpi.isg.metadata_store.domain.impl.RDBMSMetadataStore;

public class DatabaseAccessTest {

    private File testDb;
    private Connection connection;
    private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_TARGET_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Target (ID, name, locationId) VALUES (?, ?, ?);",
                    new PreparedStatementAdapter<Object[]>() {
                        @Override
                        public void translateParameter(Object[] parameters, PreparedStatement preparedStatement)
                                throws SQLException {
                            Target target = (Target) parameters[0];
                            Integer locationId = (Integer) parameters[1];
                            preparedStatement.setInt(1, target.getId());
                            preparedStatement.setString(2, target.getName());
                            preparedStatement.setInt(3, locationId);
                        }
                    },
                    "Target");

    @Before
    public void setUp() {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());

        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        RDBMSMetadataStore.createNewInstance(SQLiteInterface
                .buildAndRegisterStandardConstraints(connection));

    }

    @After
    public void tearDown() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testClosingOfDatabaseAccess() {
        DatabaseAccess dbAccess = new DatabaseAccess(connection);

        try {
            DatabaseWriter<Object[]> insertTargetWriter = dbAccess.createBatchWriter(INSERT_TARGET_WRITER_FACTORY);
            insertTargetWriter.write(new Object[] {
                    mock(Target.class), 1
            });
            dbAccess.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
