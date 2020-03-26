/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.QualifiedName;
import com.softwareplumbers.dms.RepositoryObject;

/**
 *
 * @author jonathan
 */
public class Info {
    public final Id id;
    public final Id parent_id;
    public final String name;
    public final QualifiedName path;
    public final RepositoryObject.Type type;
    
    public Info(Id id, Id parent_id, String name, QualifiedName path, RepositoryObject.Type type) {
        this.id = id;
        this.parent_id = parent_id;
        this.name = name;
        this.path = path;
        this.type = type;
    }
}
