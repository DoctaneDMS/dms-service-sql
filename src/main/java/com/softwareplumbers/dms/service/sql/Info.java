/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.dms.RepositoryObject;
import com.softwareplumbers.dms.RepositoryPath;

/**
 *
 * @author jonathan
 */
public class Info {
    public final Id id;
    public final Id parent_id;
    public final RepositoryObject.Type type;
    public final String name;
    
    public Info(Id id, Id parent_id, String name, RepositoryObject.Type type) {
        this.id = id;
        this.parent_id = parent_id;
        this.type = type;
        this.name = name;
    }
}
