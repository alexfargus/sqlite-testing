/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sqlitetest;

import com.google.common.collect.ImmutableSortedMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 *
 * @author alex
 */
public class SqliteChannel implements Closeable {

    private static final String CREATE_STATEMENT = "create table if not exists data (key integer primary key, value real) without rowid";
    private static final String INSERT_STATEMENT = "insert or replace into data (key, value) values (?, ?)";
    private static final String RETRIEVE_STATEMENT = "select key, value from data where key between ? and ?";

    protected static final Logger LOG = Logger.getLogger(SqliteChannel.class.getName());

    private final Connection connection;

    public SqliteChannel(Path root, String channelId) throws SQLException {
        final String URI = String.format("jdbc:sqlite:%s.sqlite", root.resolve(channelId).toString());
        connection = DriverManager.getConnection(URI);
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_STATEMENT);
            connection.commit();
        }

    }
    
    public synchronized void putData(final NavigableMap<Long, Object> data) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_STATEMENT)) {
            data.forEach((Long key, Object value) -> {
                try {
                    statement.setLong(1, key);
                    statement.setDouble(2, ((Number) value).doubleValue());
                    statement.addBatch();
                } catch (SQLException ex) {
                    LOG.warning(ex.getMessage());
                }
            });
            statement.executeBatch();
            connection.commit();
        }
    }
    
    public NavigableMap<Long, Object> getData(Long from, Long to) throws SQLException {
        final ImmutableSortedMap.Builder<Long, Object> builder = ImmutableSortedMap.naturalOrder();
        try (PreparedStatement statement = connection.prepareStatement(RETRIEVE_STATEMENT)) {
            statement.setLong(1, from);
            statement.setLong(2, to);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    builder.put(rs.getLong(1), rs.getDouble(2));
                }
            }
        }
        return builder.build();
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
    }

}
