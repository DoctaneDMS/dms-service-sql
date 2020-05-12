/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.sql.CompositeType;
import com.softwareplumbers.common.sql.CustomType;
import com.softwareplumbers.common.sql.FluentStatement;
import com.softwareplumbers.dms.RepositoryPath;
import java.sql.PreparedStatement;

/**
 *
 * @author jonathan
 */
public class Types {
    public static final CustomType<Id> ID = (statement, index, value) -> {
            if (value == null) statement.setNull(index,  java.sql.Types.BINARY);
            else statement.setBytes(index, value.getBytes());            
    };
    
    public static final CompositeType<RepositoryPath> PATH = Types::setRepositoryPath;
    
    public static final FluentStatement setRepositoryPath(FluentStatement fluentStatement, String name, RepositoryPath path) {
        if (path.isEmpty()) return fluentStatement.set(ID, name, Id.ROOT_ID);
        switch (path.part.type) {
            case DOCUMENT_PATH:
                RepositoryPath.VersionedElement docPart = (RepositoryPath.VersionedElement)path.part;
                return fluentStatement.set(PATH, "parent." + name, path.parent).set(name, docPart.name).set(name + ".version", docPart.version.orElse("")); 
            case DOCUMENT_ID:
                RepositoryPath.DocumentIdElement docIdPart = (RepositoryPath.DocumentIdElement)path.part;
                return fluentStatement.set(PATH, "parent." + name, path.parent).set(name, docIdPart.id).set(name + ".version", docIdPart.version.orElse("")); 
            case OBJECT_ID:
                RepositoryPath.IdElement idPart = (RepositoryPath.IdElement)path.part;
                return fluentStatement.set(name, idPart.id);
            default:
                return fluentStatement.set(PATH, "parent." + name, path.parent);
        }
    }
}

