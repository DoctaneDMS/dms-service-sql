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
import com.softwareplumbers.dms.RepositoryPath.Version;
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
    
    public static final CustomType<Version> VERSION = (statement, index, value) -> {
        if (value == null) statement.setNull(index,  java.sql.Types.VARCHAR);
        else statement.setString(index, value.getName().orElse(value.getId().orElse("")));
    };
    
    public static final CompositeType<RepositoryPath> PATH = Types::setRepositoryPath;
    
    public static final FluentStatement setRepositoryPath(FluentStatement fluentStatement, String name, RepositoryPath path) {
        if (path.isEmpty()) return fluentStatement.set(ID, name, Id.ROOT_ID);
        Version version = path.part.getVersion();
        switch (path.part.type) {
            case NAME:
                RepositoryPath.NamedElement docPart = (RepositoryPath.NamedElement)path.part;
                return fluentStatement.set(PATH, "parent." + name, path.parent).set(name, docPart.name).set(VERSION, name + ".version", version); 
            case ID:
                RepositoryPath.IdElement docIdPart = (RepositoryPath.IdElement)path.part;
                if (path.parent.isEmpty())
                    return fluentStatement.set(name, docIdPart.id); 
                else
                    return fluentStatement.set(PATH, "parent." + name, path.parent).set(name, docIdPart.id).set(VERSION, name + ".version", version);
            default:
                return fluentStatement.set(PATH, "parent." + name, path.parent);
        }
    }
}

