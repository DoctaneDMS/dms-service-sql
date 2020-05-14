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
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

/**
 *
 * @author jonathan
 */
public class DocumentDatabase extends AbstractDatabase<DocumentDatabase.Type, DocumentDatabase.Operation, DocumentDatabase.Template, DatabaseInterface> {

    public DocumentDatabase(Schema<Type> schema) {
        super(schema);
    }

    @Override
    public DatabaseInterface createInterface(Schema<Type> schema, OperationStore<Operation> os, TemplateStore<Template> ts) throws SQLException {
        return new DatabaseInterface(schema, os, ts);
    }
    
    public static enum Type {
        NODE,
        VERSION,
        FOLDER,
        LINK,
        LINK_VERSION
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
        nameExpr       
    }   
}
