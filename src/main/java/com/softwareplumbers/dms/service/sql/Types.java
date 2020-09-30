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
import java.sql.SQLException;

/**
 *
 * @author jonathan
 */
public class Types {
    public static final CustomType<Id> ID = new CustomType<Id>() {
        
        @Override
        public void set(PreparedStatement statement, int index, Id value) throws SQLException {
            if (value == null) statement.setNull(index,  java.sql.Types.BINARY);
            else statement.setBytes(index, value.getBytes());            
        };

        @Override
        public String format(Id t) {
            return t.toString();
        }
    };
    
    public static final CustomType<Version> VERSION = new CustomType<Version>() {
        
        @Override
        public void set(PreparedStatement statement, int index, Version value) throws SQLException {
            if (value == null) 
                statement.setNull(index,  java.sql.Types.VARCHAR);
            else if (value.getName().isPresent()) 
                statement.setString(index, value.getName().get());
            else if (value.getId().isPresent()) 
                statement.setBytes(index, Id.of(value.getId().get()).getBytes());
            else
                statement.setString(index, "");
        }
        
        @Override
        public String format(Version value) {
            return value.toString();
        }        
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
                    return fluentStatement.set(ID, name, Id.of(docIdPart.id)); 
                else
                    return fluentStatement.set(PATH, "parent." + name, path.parent).set(ID, name, Id.of(docIdPart.id)).set(VERSION, name + ".version", version);
            default:
                return fluentStatement.set(PATH, "parent." + name, path.parent);
        }
    }
}

