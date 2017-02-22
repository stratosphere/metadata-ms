package de.hpi.isg.mdms.rdbms;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.targets.Target;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseAccessTest {

    private File testDb;
    private Connection connection;
    private static final PreparedStatementBatchWriter.Factory<Target> INSERT_TARGET_WRITER_FACTORY =
            new PreparedStatementBatchWriter.Factory<>(
                    "INSERT INTO Target (ID, name) VALUES (?, ?);",
                    (target, preparedStatement) -> {
                        preparedStatement.setInt(1, target.getId());
                        preparedStatement.setString(2, target.getName());
                    },
                    "Target");

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
        try {
            this.testDb = File.createTempFile("test", ".db");
            this.testDb.deleteOnExit();

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());
            try (Statement stmt = this.connection.createStatement()) {
                stmt.execute("create table [Target] ([ID] int, [name] text)");
            }
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            throw e;
        }

    }

    @After
    public void tearDown() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPendingWritesOnClose() throws SQLException {
        // Write something into the DB.
        DatabaseAccess dbAccess = new DatabaseAccess(this.connection);
        DatabaseWriter<Target> insertTargetWriter = dbAccess.createBatchWriter(INSERT_TARGET_WRITER_FACTORY);
        Target target = mock(Target.class);
        when(target.getId()).thenReturn(42);
        when(target.getName()).thenReturn("My Target");
        insertTargetWriter.write(target);
        dbAccess.close();

        Assert.assertTrue(this.connection.isClosed());

        // Reconnect.
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + this.testDb.toURI().getPath());
        try (ResultSet rs = this.connection.createStatement().executeQuery("select [ID], [name] from [Target]")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(42, rs.getInt("ID"));
            Assert.assertEquals("My Target", rs.getString("name"));
            Assert.assertFalse(rs.next());
        }
    }
}
