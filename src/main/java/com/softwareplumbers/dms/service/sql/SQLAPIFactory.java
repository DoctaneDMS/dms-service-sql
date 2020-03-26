/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author jonathan
 */
public class SQLAPIFactory {
    
    private final Operations operations;
    private final Templates templates;
    private final Schema schema;
    
    @Autowired
    public SQLAPIFactory(Operations operations, Templates templates, Schema schema) {
        this.operations = operations;
        this.templates = templates;
        this.schema = schema;
    }
    
    public SQLAPI getSQLAPI() throws SQLException {
        return new SQLAPI(operations, templates, schema);
    }
    
}
