/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.sql.AbstractDatabase;
import com.softwareplumbers.common.sql.DatabaseConfig;
import com.softwareplumbers.common.sql.DatabaseConfigFactory;
import java.sql.SQLException;
import java.util.function.BiFunction;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.sql.DataSource;
import static com.softwareplumbers.dms.service.sql.DocumentDatabase.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.util.Properties;

/**
 *
 * @author jonathan
 */
public class DocumentDatabase extends AbstractDatabase<EntityType, DataType, Operation, Template, DatabaseInterface> {

    private static DataSource getDatasource(URI jdbcURI, Properties properties) throws SQLException {
        HikariConfig config = new HikariConfig();      
        config.setDataSourceProperties(properties);
        config.setJdbcUrl(jdbcURI.toString());
        config.setUsername(properties.getProperty("username"));
        config.setPassword(properties.getProperty("password"));
        if (properties.containsKey("driverClassName")) config.setDriverClassName(properties.getProperty("driverClassName"));
        if (properties.containsKey("connectionInitSql")) config.setConnectionInitSql(properties.getProperty("connectionInitSql"));        
        return new HikariDataSource(config);
    }
    
    public DocumentDatabase(DataSource datasource, DatabaseConfig<EntityType, DataType, Operation, Template> config) {
        super(datasource, config);
    }
    
    public DocumentDatabase(DataSource datasource, DatabaseConfigFactory<EntityType, DataType, Operation, Template> config, CreateOption createOption) throws SQLException {
        super(datasource, config, createOption);
    }
    
    public DocumentDatabase(URI jdbcURI, Properties properties, DatabaseConfig<EntityType, DataType, Operation, Template> config) throws SQLException {
        super(getDatasource(jdbcURI, properties), config);
    }

    public DocumentDatabase(URI jdbcURI, Properties properties, DatabaseConfigFactory<EntityType, DataType, Operation, Template> config, CreateOption createOption) throws SQLException {
        super(getDatasource(jdbcURI, properties), config, createOption);
    }    
    
    
    public DocumentDatabase() {        
    }

    @Override
    public DatabaseInterface createInterface() throws SQLException {
        return new DatabaseInterface(this);
    }
    
    public static enum EntityType {
        NODE,
        VERSION,
        FOLDER,
        VERSION_LINK,
        DOCUMENT_LINK,
        LINK
    }
    
    public static enum DataType {
        STRING,
        UUID,
        NUMBER,
        BOOLEAN,
        BINARY
    }
       
    public static enum Operation {
        fetchLatestDocument,
        fetchDocument,
        fetchChildren,
        createDocument,
        createVersion,
        createNode,
        createFolder,
        createLink,
        deleteObject,
        undeleteObject,
        deleteDocumentById,
        fetchPathToId,
        fetchLastNameLike,
        fetchChildByName,
        updateLink,
        updateFolder,
        updateDocument,
        copyFolder,
        copyLink,
        lockVersions,
        unlockVersions,
        copyNode,
        publishNode,
        publishLink,
        updateDigest, 
        purgeChild
    }
    
    public static enum Template {
        fetchDocumentLink,
        fetchFolder,
        fetchInfo,
        fetchDocument,
        nameExpr,
        documentNameExpr,
        uuidExpr
    }  
    
    public static class MySQLValueFormatter implements BiFunction<DataType, JsonValue, String> {
        @Override
        public String apply(DataType type, JsonValue value) {
            if (type == null) return defaultValueFormatter(type, value);
            switch (type) {
                case UUID:
                    return "UUID_TO_BIN('" + ((JsonString)value).getString() + "')";
                default: return defaultValueFormatter(type,value);
            }
        }
    }
}
