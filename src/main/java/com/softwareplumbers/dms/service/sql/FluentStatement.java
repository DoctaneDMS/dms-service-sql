/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.immutablelist.QualifiedName;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.RepositoryPath.DocumentIdElement;
import com.softwareplumbers.dms.RepositoryPath.IdElement;
import com.softwareplumbers.dms.RepositoryPath.VersionedElement;
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
import com.softwareplumbers.common.abstractquery.visitor.Visitors.SQLResult;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/** Utility to allow fluent construction of database statements and processing of result sets.
 *
 * @author Jonathan Essex
 */
public abstract class FluentStatement {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(FluentStatement.class);
    private String objectId(Object obj) { return Integer.toHexString(System.identityHashCode(obj)); }
    
    protected abstract SQLResult buildSQL() throws SQLException;
    protected abstract void buildStatement(PreparedStatement statement, Map<String, List<Integer>> parameters) throws SQLException;

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

    private static List<Integer> getIndexes(String name, Map<String,List<Integer>> parameters) throws SQLException {
        List<Integer> indexes = parameters.get(name);
        if (indexes == null) throw new SQLException("Unknown parameter: " + name);
        return indexes;
    }
    
    private static Map<String, List<Integer>> computeParamMap(List<String> parameters) {
        Map<String, List<Integer>> paramMap = new TreeMap<>();
        for (int i = 0; i < parameters.size(); i++) {
            String name = parameters.get(i);
            List<Integer> bucket = paramMap.computeIfAbsent(name, n -> new LinkedList<Integer>());
            bucket.add(i);
        }
        return paramMap;
    }
            
    private static class Base extends FluentStatement {
        private final SQLResult sql;
        @Override
        protected SQLResult buildSQL() throws SQLException {
            return sql;
        }
        @Override
        protected void buildStatement(PreparedStatement statement, Map<String,List<Integer>> parameters) throws SQLException {            
        }
        public Base(SQLResult sql) { this.sql = sql; }
        

    }
    
    private static abstract class NamedParam<T> extends FluentStatement {
        protected final FluentStatement base;
        protected final T value;
        protected final String name;
        @Override
        protected SQLResult buildSQL() throws SQLException {
            return base.buildSQL();
        }
        public NamedParam(FluentStatement base, String name, T value) {
            this.base = base; this.name = name; this.value = value;
        }
    }
    
    private static abstract class IndexedParam<T> extends FluentStatement {
        protected final FluentStatement base;
        protected final T value;
        protected final int index;
        @Override
        protected SQLResult buildSQL() throws SQLException {
            return base.buildSQL();
        }
        public IndexedParam(FluentStatement base, int index, T value) {
            this.base = base; this.index = index; this.value = value;
        }
    }
    
    private static class NullParam extends FluentStatement {
        protected final FluentStatement base;
        protected final int type;
        protected final String name;
        @Override
        protected SQLResult buildSQL() throws SQLException {
            return base.buildSQL();
        }
        public NullParam(FluentStatement base, String name, int type) {
            this.base = base; this.name = name; this.type = type;
        }     

        @Override
        protected void buildStatement(PreparedStatement statement, Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to NULL", name);
            for (int index : getIndexes(name, parameters))
                statement.setNull(index+1, type);
        }
    }
    
    
    private static class NullIndexedParam extends FluentStatement {
        protected final FluentStatement base;
        protected final int type;
        protected final int index;
        @Override
        protected SQLResult buildSQL() throws SQLException {
            return base.buildSQL();
        }
        public NullIndexedParam(FluentStatement base, int index, int type) {
            this.base = base; this.index = index; this.type = type;
        }     

        @Override
        protected void buildStatement(PreparedStatement statement, Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to NULL", index);
            statement.setNull(index, type);
        }
    }
    
    private static class StringParam extends NamedParam<String> {
        public StringParam(FluentStatement base, String name, String value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement, Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", name, value);
            for (int index : getIndexes(name, parameters))
                statement.setString(index+1, value);
        } 
    }

    private static class StringIndexedParam extends IndexedParam<String> {
        public StringIndexedParam(FluentStatement base, int name, String value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement, Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", index, value);
            statement.setString(index, value);
        } 
    }
    
    private static class LongParam extends NamedParam<Long> {
        public LongParam(FluentStatement base, String name, long value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", name, value);
            for (int index : getIndexes(name, parameters))
                statement.setLong(index+1, value);
        } 
    }

    private static class LongIndexedParam extends IndexedParam<Long> {
        public LongIndexedParam(FluentStatement base, int index, long value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", index, value);
            statement.setLong(index, value);
        } 
    }
    
    private static class BooleanParam extends NamedParam<Boolean> {
        public BooleanParam(FluentStatement base, String name, boolean value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", name, value);
            for (int index : getIndexes(name, parameters))
                statement.setBoolean(index+1, value);
        } 
    }
    
    private static class BooleanIndexedParam extends IndexedParam<Boolean> {
        public BooleanIndexedParam(FluentStatement base, int index, boolean value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", index, value);
            statement.setBoolean(index, value);
        } 
    }

    private static class BinaryParam extends NamedParam<byte[]> {
        public BinaryParam(FluentStatement base, String name, byte[] value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", name, value);
            for (int index : getIndexes(name, parameters))
                statement.setBytes(index+1, value);
        } 
    }

    private static class BinaryIndexedParam extends IndexedParam<byte[]> {
        public BinaryIndexedParam(FluentStatement base, int index, byte[] value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", index, value);
            statement.setBytes(index, value);
        } 
    }
    
    private static class IdParam extends NamedParam<Id> {
        public IdParam(FluentStatement base, String name, Id value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", name, value);
            for (int index : getIndexes(name, parameters)) {
                if (value == null) statement.setNull(index, Types.BINARY);
                else statement.setBytes(index+1, value.getBytes());
            }
        } 
    }

    private static class IdIndexedParam extends IndexedParam<Id> {
        public IdIndexedParam(FluentStatement base, int index, Id value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to {}", index, value);
            if (value == null) statement.setNull(index, Types.BINARY);
            else statement.setBytes(index, value.getBytes());
        } 
    }

    private static class ClobParam extends NamedParam<Consumer<Writer>> {
        public ClobParam(FluentStatement base, String name, Consumer<Writer> value) { super(base, name, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
            LOG.debug("setting param {} to <character stream>", name);
            Clob clob = statement.getConnection().createClob();
            value.accept(clob.setCharacterStream(1));
            for (int index : getIndexes(name, parameters)) 
                statement.setClob(index+1, clob);
        }
   
    }
    
    private static class ClobIndexedParam extends IndexedParam<Consumer<Writer>> {
        public ClobIndexedParam(FluentStatement base, int index, Consumer<Writer> value) { super(base, index, value); }
        @Override
        protected void buildStatement(PreparedStatement statement,  Map<String, List<Integer>> parameters) throws SQLException {
            base.buildStatement(statement, parameters);
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
        SQLResult sql = buildSQL();
        LOG.debug(sql.sql);
        try (PreparedStatement statement = con.prepareStatement(sql.sql)) {

            buildStatement(statement, computeParamMap(sql.parameters));
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
        SQLResult sql = buildSQL();
        LOG.debug(sql.sql);
        PreparedStatement statement = con.prepareStatement(sql.sql); 
        buildStatement(statement, computeParamMap(sql.parameters));
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
        SQLResult sql = buildSQL();
        LOG.debug(sql.sql);
        PreparedStatement statement = con.prepareStatement(sql.sql);
        buildStatement(statement, computeParamMap(sql.parameters));
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
        return new Base(new SQLResult(sql, Collections.EMPTY_LIST));
    }
    
    /** Create a fluent statement from an SQL string 
     * 
     * @param sql SQL to execute (may include parameters ('?'))
     * @return A fluent statement for further manipulation/execution
     */
    public static FluentStatement of(SQLResult sql) {
        return new Base(sql);
    }
    
    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, String value) { return new StringParam(this, name, value); }

    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, String value) { return new StringIndexedParam(this, index, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, long value) { return new LongParam(this, name, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, long value) { return new LongIndexedParam(this, index, value); }

    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, boolean value) { return new BooleanParam(this, name, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, boolean value) { return new BooleanIndexedParam(this, index, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */    
    public FluentStatement set(String name, byte[] value) { return new BinaryParam(this, name, value); }
    
    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */    
    public FluentStatement set(int index, byte[] value) { return new BinaryIndexedParam(this, index, value); }

    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, Consumer<Writer> value) { return new ClobParam(this, name, value); }
    
     /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, Consumer<Writer> value) { return new ClobIndexedParam(this, index, value); }
    
    /** Set the given parameter to null 
     * 
     * @param index Index of parameter to set
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement setNull(String name) { return new NullParam(this, name, Types.NULL); }
    
    /** Set several parameters to values supplied by a qualified name.
     * 
     * Will set the parameter at the index give to the value of name.part (the last part of the qualified name).
     * Sets following parameters to successive parent parts of the qualified name.
     * 
     * @param name Name of parameter to set
     * @param path Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, RepositoryPath path) { 
        if (path.isEmpty()) return set(name, Id.ROOT_ID);
        switch (path.part.type) {
            case DOCUMENT_PATH:
                VersionedElement docPart = (VersionedElement)path.part;
                return set("parent." + name, path.parent).set(name, docPart.name).set(name + ".version", docPart.version.orElse("")); 
            case DOCUMENT_ID:
                DocumentIdElement docIdPart = (DocumentIdElement)path.part;
                return set("parent." + name, path.parent).set(name, docIdPart.id).set(name + ".version", docIdPart.version.orElse("")); 
            case OBJECT_ID:
                IdElement idPart = (IdElement)path.part;
                return set(name, idPart.id);
            default:
                return set("parent." + name, path.parent);
        }
    }

    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param id Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, Id id) { return new IdParam(this, name, id); }

    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param id Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, Id id) { return new IdIndexedParam(this, index, id); }

    /** Set the given parameter to the given value
     * 
     * @param name Name of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(String name, JsonObject value) { return new ClobParam(this, name, out-> { try (JsonWriter writer = Json.createWriter(out)) { writer.write(value);} }); }

    /** Set the given parameter to the given value
     * 
     * @param index Index of parameter to set
     * @param value Value to which we will set the parameter
     * @return A fluent statement for further manipulation/execution
     */
    public FluentStatement set(int index, JsonObject value) { return new ClobIndexedParam(this, index, out-> { try (JsonWriter writer = Json.createWriter(out)) { writer.write(value);} }); }

    /** Set optional parameter value
     * 
     * @param <T> Type of value to set
     * @param index Index of parameter to set
     * @param optValue Value to set
     * @return  A fluent statement for further manipulation/execution
     */
    public <T> FluentStatement set(String name, Optional<T> optValue) {
        if (optValue.isPresent()) {
            T value = optValue.get();
            if (value instanceof String) return set(name, (String)value);
            if (value instanceof Id) return set(name, (Id)value);
            if (value instanceof RepositoryPath) return set(name, (RepositoryPath)value);
            if (value instanceof byte[]) return set(name, (byte[])value);
            if (value instanceof Boolean) return set(name, (Boolean)value);
            if (value instanceof Long) return set(name, (Long)value);
            throw new RuntimeException("unsupported optional type: " + value.getClass());
        }
        return this;
    }
    
    private static int countParameters(int initCount, RepositoryPath.Element element) {
        switch (element.type) {
            case DOCUMENT_PATH: 
                return initCount+2;
            case DOCUMENT_ID:
                DocumentIdElement docId = (DocumentIdElement)element;
                return docId.version == null ? initCount+1 : initCount+2;
            default: 
                return initCount+1;
        }
    }
    
    public static int countParameters(RepositoryPath path) {
        return path.apply(0, FluentStatement::countParameters);
    }
}
