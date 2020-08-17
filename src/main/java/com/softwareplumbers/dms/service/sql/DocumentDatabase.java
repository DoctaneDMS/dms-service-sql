/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.sql.AbstractDatabase;
import com.softwareplumbers.common.sql.OperationStore;
import com.softwareplumbers.common.sql.Schema;
import com.softwareplumbers.common.sql.TemplateStore;
import java.sql.SQLException;
import java.util.function.BiFunction;
import javax.json.JsonString;
import javax.json.JsonValue;

/**
 *
 * @author jonathan
 */
public class DocumentDatabase extends AbstractDatabase<DocumentDatabase.EntityType, DocumentDatabase.DataType, DocumentDatabase.Operation, DocumentDatabase.Template, DatabaseInterface> {

    public DocumentDatabase(Schema<EntityType, DataType> schema) {
        super(schema);
    }

    @Override
    public DatabaseInterface createInterface(Schema<EntityType, DataType> schema, OperationStore<Operation> os, TemplateStore<Template> ts) throws SQLException {
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
        publishLink        
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
