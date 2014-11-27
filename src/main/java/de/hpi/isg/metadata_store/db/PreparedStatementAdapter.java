package de.hpi.isg.metadata_store.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementAdapter<T> {

    void translateParameter(T object, PreparedStatement preparedStatement) throws SQLException;

}