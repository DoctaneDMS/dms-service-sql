/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.immutablelist.QualifiedName;
import com.softwareplumbers.common.abstractquery.visitor.Formatter;
import com.softwareplumbers.common.abstractquery.visitor.Visitors;
import com.softwareplumbers.common.abstractquery.visitor.Visitors.SQLFormat.Relationship;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author jonathan
 */
public class Schema {
    
    private static List<QualifiedName> fieldList(QualifiedName... fields) {
        return Collections.unmodifiableList(Arrays.asList(fields));
    }
    
    private static List<QualifiedName> fieldList(List<QualifiedName> base, QualifiedName... fields) {
        ArrayList<QualifiedName> list = new ArrayList<>(base);
        list.addAll(Arrays.asList(fields));
        return Collections.unmodifiableList(list);
    }
    
    private static List<QualifiedName> fieldList(List<QualifiedName> base1, List<QualifiedName> base2) {
        ArrayList<QualifiedName> list = new ArrayList<>(base1);
        list.addAll(base2);
        return Collections.unmodifiableList(list);
    }
    
    private static Map.Entry<QualifiedName,String> mapEntry(String dbname, String... qname) {
        return new AbstractMap.SimpleImmutableEntry<>(QualifiedName.of(qname), dbname);
    }
    
    private static Map<QualifiedName, String> nameMap(Map.Entry<QualifiedName,String>... entries) {
        HashMap<QualifiedName, String> map = new HashMap<>();
        Stream.of(entries).forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(map);
    }
    
    private static Map<QualifiedName, String> nameMap(Map<QualifiedName, String> base, Map.Entry<QualifiedName,String>... entries) {
        HashMap<QualifiedName, String> map = new HashMap<>();
        map.putAll(base);
        Stream.of(entries).forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(map);
    }

    private static final List<QualifiedName> DOCUMENT_FIELDS = fieldList(
        QualifiedName.of("id"),
        QualifiedName.of("version"),
        QualifiedName.of("mediaType"),
        QualifiedName.of("digest"),
        QualifiedName.of("length"),
        QualifiedName.of("latest")
    );
    
    private static final List<QualifiedName> NODE_FIELDS = fieldList(
        QualifiedName.of("parentId"),
        QualifiedName.of("name"),
        QualifiedName.of("deleted")
    );

    private static final List<QualifiedName> FOLDER_FIELDS = fieldList(NODE_FIELDS,
       QualifiedName.of("id"),
       QualifiedName.of("state")
    );
    
    private static final List<QualifiedName> LINK_FIELDS = fieldList(NODE_FIELDS, DOCUMENT_FIELDS);
    
    private static final Map<QualifiedName, String> NODE_NAME_MAP = nameMap(
        mapEntry("PARENT_ID", "parentId"),
        mapEntry("NAME", "name"),
        mapEntry("DELETED", "deleted")
    );
    
    private static final Map<QualifiedName, String> FOLDER_NAME_MAP = nameMap(NODE_NAME_MAP,
        mapEntry("STATE", "state"),
        mapEntry("ID", "id")
    );

    private static final Map<QualifiedName, String> DOCUMENT_NAME_MAP = nameMap(
        mapEntry("ID", "id"),
        mapEntry("VERSION", "version"),
        mapEntry("MEDIA_TYPE", "mediaType"),
        mapEntry("DIGEST", "digest"),
        mapEntry("LENGTH", "length"),
        mapEntry("LATEST", "latest")
    );

    private static final Map<QualifiedName, String> LINK_NAME_MAP = nameMap(NODE_NAME_MAP,
        mapEntry("DOCUMENT_ID", "id"),
        mapEntry("VERSION_ID", "version"),
        mapEntry("MEDIA_TYPE", "mediaType"),
        mapEntry("DIGEST", "digest"),
        mapEntry("LENGTH", "length")
    );
    
    private static final String mapLinkName(QualifiedName qname) {
        if (qname.isEmpty()) return "VIEW_LINKS";
        if (qname.get(0).equals("parent")) return mapFolderName(qname.rightFromStart(1));
        return LINK_NAME_MAP.get(qname);
    }
    
    private static final String mapLinkVersionName(QualifiedName qname) {
        if (qname.isEmpty()) return "VIEW_LINK_VERSIONS";
        if (qname.get(0).equals("parent")) return mapFolderName(qname.rightFromStart(1));
        return LINK_NAME_MAP.get(qname);
    }
    
    private static final String mapFolderName(QualifiedName qname) {
        if (qname.isEmpty()) return "VIEW_FOLDERS";
        if (qname.get(0).equals("parent")) return mapFolderName(qname.rightFromStart(1));
        return FOLDER_NAME_MAP.get(qname);        
    }
    
    private static final String mapNodeName(QualifiedName qname) {
        if (qname.isEmpty()) return "NODES";
        if (qname.equals(QualifiedName.of("id"))) return "ID";
        if (qname.get(0).equals("parent")) return mapFolderName(qname.rightFromStart(1));
        return NODE_NAME_MAP.get(qname);        
    }
    
    private static final String mapDocumentName(QualifiedName qname) {
        if (qname.isEmpty()) return "VIEW_DOCUMENTS";
        return DOCUMENT_NAME_MAP.get(qname);        
    }
    
    private static final String parentRelationship(QualifiedName parent, Map<QualifiedName, String> tableNames) {
        return tableNames.get(parent.parent)+".PARENT_ID = " + tableNames.get(parent) + ".ID";
    }
  
    private static final Relationship getRelationship(QualifiedName table) {
        if ("parent".equals(table.part))
            return tableNames->parentRelationship(table, tableNames);
        return null;
    }
    
    private static final Formatter<String> LINK_FORMATTER = Visitors.SQL(Schema::mapLinkName, Schema::getRelationship);
    private static final Formatter<String> LINK_VERSION_FORMATTER = Visitors.SQL(Schema::mapLinkVersionName, Schema::getRelationship);
    private static final Formatter<String> FOLDER_FORMATTER = Visitors.SQL(Schema::mapFolderName, Schema::getRelationship);
    private static final Formatter<String> NODE_FORMATTER = Visitors.SQL(Schema::mapNodeName, Schema::getRelationship);
    private static final Formatter<String> DOCUMENT_FORMATTER = Visitors.SQL(Schema::mapDocumentName, name->null);
    
    private String updateScript;
    private String createScript;
    private String dropScript;

    
    public void setCreateScript(String script) { createScript=script; }
    public void setUpdateScript(String script) { updateScript=script; }
    public void setDropScript(String script) { dropScript=script; }
    
    public final DataSource datasource;
    
    @Autowired
    public Schema(DataSource datasource) {
        this.datasource = datasource;
    }

    public void dropSchema() throws SQLException {
        try (
            Connection con = datasource.getConnection();
            Statement stmt = con.createStatement()
        ) {
            stmt.execute(dropScript);
        }        
    }

    public void createSchema() throws SQLException {
        try (
            Connection con = datasource.getConnection();
            Statement stmt = con.createStatement()
        ) {
            stmt.execute(createScript);
        }        
    }

    public void updateSchema() throws SQLException {
        try (
            Connection con = datasource.getConnection();
            Statement stmt = con.createStatement()
        ) {
            stmt.execute(updateScript);
        }        
    }
    
    public Iterable<QualifiedName> getLinkFields() {
        return LINK_FIELDS;
    }
    
    public Iterable<QualifiedName> getFolderFields() {
        return FOLDER_FIELDS;
    }
    
    public Iterable<QualifiedName> getDocumentFields() {
        return DOCUMENT_FIELDS;
    }
    
    public Formatter<String> getLinkFormatter(boolean withVersions) {
        return withVersions ? LINK_VERSION_FORMATTER : LINK_FORMATTER;
    }
    
    public Formatter<String> getFolderFormatter() {
        return FOLDER_FORMATTER;
    }
    
    public Formatter<String> getNodeFormatter() {
        return NODE_FORMATTER;
    }
    
    public Formatter<String> getDocumentFormatter() {
        return DOCUMENT_FORMATTER;
    }
}
