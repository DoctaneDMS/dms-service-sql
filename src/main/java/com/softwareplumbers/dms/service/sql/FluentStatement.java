/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.QualifiedName;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import javax.sql.DataSource;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Utility to allow fluent construction of database statements and processing of result sets.
 *
 * @author Jonathan Essex
 */
public abstract class FluentStatement {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(FluentStatement.class);
    private String objectId(Object obj) { return Integer.toHexString(System.identityHashCode(obj)); }
    
    protected abstract String buildSQL() throws SQLException;
    protected abstract void buildStatement(PreparedStatement statement) throws SQLException;

    private static int connectionCount = 0;
    
    private void logOpen(Connection con) {
        if (LOG.isDebugEnabled()) {
            synchronized(this) {
                connectionCount++;
            }
            LOG.debug("Opening connection {}: connection count: {}", objectId(con), connectionCount);
        }
    }
    
    private void logClose(Connection con) {
        if (LOG.isDebugEnabled()) {
            synchronized(this) {
                connectionCount--;
            }
            LOG.debug("Opening connection {}: connection count: {}", objectId(con), connectionCount);
        }
    }
    
    /** Get the count of open database connections which are managed via the FluentStatement API.
     * 
     * This will return 0 unless the logger for this class is set to DEBUG.
     * 
     * @return The count of currently open database connections.
     */
    public static int getConnectionCount() {
        return connectionCount;
    }

    private static class Base extends FluentStatement {
        private final String sql;
        @Override
        protected String buildSQL() throws SQLException {
            return sql;
        }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {            
        }
        public Base(String sql) { this.sql = sql; }
    }
    
    private static abstract class Param<T> extends FluentStatement {
        protected final FluentStatement base;
        protected final T value;
        protected final int index;
        @Override
        protected String buildSQL() throws SQLException {
            return base.buildSQL();
        }
        public Param(FluentStatement base, int index, T value) {
            this.base = base; this.index = index; this.value = value;
        }
    }
    
    private static class StringParam extends Param<String> {
        public StringParam(FluentStatement base, int index, String value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {
            base.buildStatement(statement);
            LOG.debug("setting param {} to {}", index, value);
            statement.setString(index, value);
        } 
    }
    
    private static class LongParam extends Param<Long> {
        public LongParam(FluentStatement base, int index, long value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {
            base.buildStatement(statement);
            LOG.debug("setting param {} to {}", index, value);
            statement.setLong(index, value);
        } 
    }
    
    private static class BooleanParam extends Param<Boolean> {
        public BooleanParam(FluentStatement base, int index, boolean value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {
            base.buildStatement(statement);
            LOG.debug("setting param {} to {}", index, value);
            statement.setBoolean(index, value);
        } 
    }
    
    private static class BinaryParam extends Param<byte[]> {
        public BinaryParam(FluentStatement base, int index, byte[] value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {
            base.buildStatement(statement);
            LOG.debug("setting param {} to {}", index, value);
            statement.setBytes(index, value);
        } 
    }
    
    private static class IdParam extends Param<Id> {
        public IdParam(FluentStatement base, int index, Id value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {
            base.buildStatement(statement);
            LOG.debug("setting param {} to {}", index, value);
            if (value == null) statement.setNull(index, Types.BINARY);
            else statement.setBytes(index, value.getBytes());
        } 
    }

    private static class ClobParam extends Param<Consumer<Writer>> {
        public ClobParam(FluentStatement base, int index, Consumer<Writer> value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement) throws SQLException {
            base.buildStatement(statement);
            LOG.debug("setting param {} to <character stream>", index);
            Clob clob = statement.getConnection().createClob();
            value.accept(clob.setCharacterStream(1));
            statement.setClob(index, clob);
        } 
    }

    /** Execute this statement on the given unmanaged connection.
     * 
     * The connection remains open after execution completes.
     * 
     * @param con Connection on which to execute statement.
     * @return Update count
     * @throws SQLException 
     */
    public int execute(Connection con) throws SQLException {
        String sql = buildSQL();
        LOG.debug(sql);
        try (PreparedStatement statement = con.prepareStatement(sql)) {
            buildStatement(statement);
            return statement.executeUpdate();
        }
    }
    
    /** Execute this statement on the given unmanaged connection.
     * 
     * The connection remains open after execution completes. The JDBC statement
     * object will remain open until the result stream is closed.
     * 
     * This form should only be used where we know that the result stream
     * will be consumed and closed before the connection is closed.
     * 
     * @param <T> Type of object to create for each result row
     * @param con Connection on which to execute the statement
     * @param mapper Mapper converts each row of the result set into an object of type T
     * @return A stream of result objects
     * @throws SQLException 
     */
    public <T> Stream<T> execute(Connection con, Mapper<T> mapper) throws SQLException {
        String sql = buildSQL();
        LOG.debug(sql);
        PreparedStatement statement = con.prepareStatement(sql); 
        buildStatement(statement);
        LOG.debug("built statement: {}", objectId(statement));
        ResultSetIterator<T> iterator = new ResultSetIterator(statement.executeQuery(), mapper);
        Stream<T> result = iterator.toStream().onClose(()->{ 
            LOG.debug("closing statement: {}", objectId(statement));
            try { statement.close(); } catch (SQLException e) { }
        });
        return result;
    }
    
    /** Execute this statement on a managed connection.
     * 
     * This method will open a database connection, execute the statement, and
     * hold both the connection and statement open until the result stream is finally
     * closed.
     * 
     * @param <T> Type of object to create for each result row
     * @param ds DataSource on which to execute the statement
     * @param mapper Mapper converts each row of the result set into an object of type T
     * @return A stream of result objects
     * @throws SQLException 
     */
    public <T> Stream<T> execute(DataSource ds, Mapper<T> mapper) throws SQLException {
        Connection con = ds.getConnection();
        logOpen(con);
        String sql = buildSQL();
        LOG.debug(sql);
        PreparedStatement statement = con.prepareStatement(sql);
        buildStatement(statement);
        LOG.debug("built statement: {}", objectId(statement));
        ResultSetIterator<T> iterator = new ResultSetIterator(statement.executeQuery(), mapper);
        Stream<T> result = iterator.toStream().onClose(()->{ 
            LOG.debug("closing statement: {}", objectId(statement));
            try { statement.close(); } catch (SQLException e) { }
            LOG.debug("closing connection: {}", objectId(con));
            try { con.close(); logClose(con); } catch (SQLException e) { }
        });
        return result;
    }

    /** Create a fluent statement from an SQL string 
     * 
     * @param sql SQL to execute (may include parameters ('?'))
     * @return A fluent statement for further manipulation/execution
     */
    public static FluentStatement of(String sql) {
        return new Base(sql);
    }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, String value) { return new StringParam(this, index, value); }
    
        /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, long value) { return new LongParam(this, index, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, boolean value) { return new BooleanParam(this, index, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */    
    public FluentStatement set(int index, byte[] value) { return new BinaryParam(this, index, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, Consumer<Writer> value) { return new ClobParam(this, index, value); }
    
    /** Set several parameters to values supplied by a qualified name.
     * 
     * Will set the parameter at the index give to the value of name.part (the last part of the qualified name).
     * Sets following parameters to successive parent parts of the qualified name.
     * 
     * @param index Index of parameter to set
     * @param name Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, QualifiedName name) { return name.isEmpty() ? this : set(index+1, name.parent).set(index, name.part); }

    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param id Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, Id id) { return new IdParam(this, index, id); }

    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, JsonObject value) { return new ClobParam(this, index, out-> { try (JsonWriter writer = Json.createWriter(out)) { writer.write(value);} }); }

    /** Set optional parameter value
     * 
     * @param <T> Type of value to set
     * @param index Index of parameter to set
     * @param optValue Value to set
     * @return  A fluent statement for further manipulation/execution
     */
    public <T> FluentStatement set(int index, Optional<T> optValue) {
        if (optValue.isPresent()) {
            T value = optValue.get();
            if (value instanceof String) return set(index, (String)value);
            if (value instanceof Id) return set(index, (Id)value);
            if (value instanceof QualifiedName) return set(index, (QualifiedName)value);
            if (value instanceof byte[]) return set(index, (byte[])value);
            if (value instanceof Boolean) return set(index, (Boolean)value);
            if (value instanceof Long) return set(index, (Long)value);
            throw new RuntimeException("unsupported optional type: " + value.getClass());
        }
        return this;
    }
}
