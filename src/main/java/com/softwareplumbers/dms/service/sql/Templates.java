/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.abstractquery.visitor.Visitors.SQLResult;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;

/**
 *
 * @author jonathan
 */
public class Templates {
    
    public String fetchDocumentLink;
    public String fetchFolder;
    public String fetchInfo;
    public String fetchDocument;
    public String nameExpr;

    public void setFetchDocumentLink(String template) {
        fetchDocumentLink = template;
    }

    public void setFetchFolder(String template) {
        fetchFolder = template;
    }

    public void setFetchInfo(String template) {
        fetchInfo = template;
    }
    
    public void setFetchDocument(String template) {
        fetchDocument = template;
    }
    
    public void setNameExpr(String template) {
        nameExpr = template;
    }

    public static String substitute(String template, Object... parameters) {
        StringLookup lookup = (key) -> {
            int ix = Integer.parseInt(key);
            return parameters[ix].toString();
        };
        StringSubstitutor substitutor = new StringSubstitutor(lookup,"!{","}",'\\');
        return substitutor.replace(template);
    }  
}
