package de.hpi.isg.mdms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;

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
    public void setUp() throws ClassNotFoundException, SQLException {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();

        } catch (IOException e) {
            e.printStackTrace();
        }
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());

        RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection));

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
            insertTargetWriter.write(new Object[]{
                    mock(Target.class), 1
            });
            dbAccess.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
