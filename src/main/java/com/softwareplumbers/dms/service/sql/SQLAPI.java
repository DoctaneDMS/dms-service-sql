/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.immutablelist.QualifiedName;
import com.softwareplumbers.common.abstractquery.Param;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.common.abstractquery.Range;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Exceptions.InvalidDocumentId;
import com.softwareplumbers.dms.Exceptions.InvalidObjectName;
import com.softwareplumbers.dms.Exceptions.InvalidWorkspace;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryObject;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.DocumentImpl;
import com.softwareplumbers.dms.common.impl.DocumentLinkImpl;
import com.softwareplumbers.dms.common.impl.LocalData;
import com.softwareplumbers.dms.common.impl.WorkspaceImpl;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.sql.DataSource;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author jonathan
 */
public class SQLAPI implements AutoCloseable {
    
    private static XLogger LOG = XLoggerFactory.getXLogger(SQLAPI.class);
    
    private static final String SAFE_CHARACTERS ="0123456789ABCDEFGHIJKLMNOPQURSTUVWYXabcdefghijklmnopqrstuvwxyz";
    private static final int MAX_SAFE_CHAR='z';
    private static final byte[] ROOT_ID = new byte[] { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

    public static JsonObject toJson(Reader reader) {
        try (JsonReader jsonReader = Json.createReader(reader)) {
            return jsonReader.readObject();
        }
    }
    
    public static final Mapper<Id> GET_ID = results -> {
        return new Id(results.getBytes("ID"));
    };
    
    public static final Mapper<String> GET_NAME = results -> {
        return results.getString("NAME");
    };

    public static final Mapper<Document> GET_DOCUMENT = results -> {
        String mediaType = results.getString("MEDIA_TYPE");
        Id id = new Id(results.getBytes("ID"));
        Id version = new Id(results.getBytes("VERSION_ID"));
        long length = results.getLong("LENGTH");
        byte[] hash = results.getBytes("DIGEST");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        return new DocumentImpl(new Reference(id.toString(),version.toString()), mediaType, length, hash, metadata, false, LocalData.NONE);
    };
    
    public static final Mapper<Reference> GET_REFERENCE = results -> {
        Id id = new Id(results.getBytes("ID"));
        Id version = new Id(results.getBytes("VERSION_ID"));
        return new Reference(id.toString(),version.toString());        
    };
    
    private static DocumentLink getLink(ResultSet results, QualifiedName basePath) throws SQLException {
        String mediaType = results.getString("MEDIA_TYPE");
        Id id = new Id(results.getBytes("DOCUMENT_ID"));
        String version = results.getString("VERSION_ID");
        long length = results.getLong("LENGTH");
        byte[] hash = results.getBytes("DIGEST");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        QualifiedName name = basePath.addParsed(Function.identity(), results.getString("PATH"),"/");
        return new DocumentLinkImpl(name, new Reference(id.toString(),version), mediaType, length, hash, metadata, false, LocalData.NONE);
    }
    
    public static Mapper<DocumentLink> GET_LINK_WITH_PATH(QualifiedName path) {
        return rs -> getLink(rs, path);
    }
    
    public static Consumer<Writer> clobWriter(JsonObject metadata) {
        return out -> { 
            try (JsonWriter writer = Json.createWriter(out)) { 
                writer.write(metadata); 
            } 
        };
    }
    
    public DocumentLink getLink(ResultSet results) throws SQLException {
        String mediaType = results.getString("MEDIA_TYPE");
        Id id = new Id(results.getBytes("DOCUMENT_ID"));
        String version = results.getString("VERSION_ID");
        long length = results.getLong("LENGTH");
        byte[] hash = results.getBytes("DIGEST");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        Id nodeId = new Id(results.getBytes("ID"));
        
        try (Stream<QualifiedName> names = FluentStatement
            .of(operations.fetchPathToId)
            .set(1, nodeId)
            .execute(results.getStatement().getConnection(), rs->QualifiedName.parse(rs.getString(1),"/"))
        ) {

            QualifiedName name = 
                    names.findFirst()
                    .orElseThrow(()->new RuntimeException("can't find id"));
        
            return new DocumentLinkImpl(name, new Reference(id.toString(),version), mediaType, length, hash, metadata, false, LocalData.NONE);
        }
    }

    public static Workspace getWorkspace(ResultSet results, QualifiedName basePath) throws SQLException {
        Id id = new Id(results.getBytes("ID"));
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        Workspace.State state = Workspace.State.valueOf(results.getString("STATE"));
        QualifiedName path = basePath.addParsed(Function.identity(), results.getString("PATH"),"/");
        return new WorkspaceImpl(path, id.toString(), state, metadata, false, LocalData.NONE);
    }
    
    public Workspace getWorkspace(ResultSet results) throws SQLException {
        Id id = new Id(results.getBytes("ID"));
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        Workspace.State state = Workspace.State.valueOf(results.getString("STATE"));
       
        try (Stream<QualifiedName> names = FluentStatement
            .of(operations.fetchPathToId)
            .set(1, id)
            .execute(results.getStatement().getConnection(), rs->QualifiedName.parse(rs.getString(1),"/"))
        ) {
            QualifiedName name = 
                    names.findFirst()
                    .orElseThrow(()->new RuntimeException("can't find id"));
        
            return new WorkspaceImpl(name, id.toString(), state, metadata, false, LocalData.NONE);
        }

    }
    
    public static class Timestamped<T> {
        public final Timestamp timestamp;
        public final T value;
        public Timestamped(Timestamp timestamp, T value) { this.timestamp = timestamp; this.value = value; }
    }
    
    public static <T> Mapper<Timestamped<T>> getTimestamped(Mapper<T> mapper) throws SQLException {
        return results->new Timestamped<T>(results.getTimestamp("CREATED"), mapper.map(results));
    }
    
    
    
    public static Mapper<Info> GET_INFO = results -> {
        Id id = new Id(results.getBytes("ID"));
        Id parent_id = new Id(results.getBytes("PARENT_ID"));
        String name = results.getString("NAME");
        RepositoryObject.Type type = RepositoryObject.Type.valueOf(results.getString("TYPE"));
        QualifiedName path = QualifiedName.parse(results.getString("PATH"),"/");
        return new Info(id, parent_id, name, path, type);
    };
    

    
    @Autowired
    public SQLAPI(Operations operations, Templates templates, Schema schema) throws SQLException {
        this.operations = operations;
        this.templates = templates;
        this.schema = schema;
        this.con = schema.datasource.getConnection();
    }
    
    Operations operations;
    Templates templates;
    Schema schema;
    Connection con;
    
    Query getParameterizedNameQuery(int depth) {
        if (depth == 0) return Query.from("id", Range.equals(Param.from("0")));
        if (depth == 1) return Query.from("parentId", Range.equals(Param.from("0"))).intersect(Query.from("name", Range.equals(Param.from("1"))));
        Query result = Query.from("parent",getParameterizedNameQuery(depth-1)).intersect(Query.from("name", Range.equals(Param.from(Integer.toString(depth)))));
        return result;
    }
    
    Query getNameQuery(QualifiedName name) {
        if (name.isEmpty()) return Query.UNBOUNDED;
        Query result = Query.from("parent", getNameQuery(name.parent)).intersect(Query.from("name", Range.like(name.part)));
        return result;
    }    
    
    String getNameExpression(int depth) {
        StringBuilder builder = new StringBuilder();
        builder.append("'/'");      
        for (int i = depth - 1; i >= 0 ; i--)
            builder.append(" || '/' || T").append(i).append(".NAME");
        return builder.toString();
    }
    
    Query getDBFilterExpression(Iterable<QualifiedName> validFields, Query filter) {
        return StreamSupport.stream(validFields.spliterator(), false).reduce(Query.UNBOUNDED, (query, name) -> query.intersect(filter.getConstraint(name)), (query1, query2)->query1.intersect(query2));
    }
    
    String getInfoSQL(int depth) {
        return Templates.substitute(templates.fetchInfo, getNameExpression(depth), getParameterizedNameQuery(depth).toExpression(schema.getNodeFormatter()));
    }
    
    String getDocumentLinkSQL(int depth, Optional<String> version) {
        Query query = getParameterizedNameQuery(depth);
        if (version.isPresent()) {
            query = query.intersect(Query.from("version", Range.equals(Param.from(Integer.toString(depth+1)))));
        }
        return Templates.substitute(templates.fetchDocumentLink, getNameExpression(depth), query.toExpression(schema.getLinkFormatter(version.isPresent())));
    }
    
    String getDocumentSearchSQL(Query query, boolean searchHistory) {
        query = getDBFilterExpression(schema.getDocumentFields(), query);
        if (!searchHistory) query = query.intersect(Query.from("latest", Range.equals(JsonValue.TRUE)));
        return Templates.substitute(templates.fetchDocument, query.toExpression(schema.getDocumentFormatter()));
    }
    
    String getDocumentSearchHistorySQL(Query query) {
        query = getDBFilterExpression(schema.getDocumentFields(), query);
        query = query.intersect(Query.from("id", Range.equals(Param.from("0"))));
        return Templates.substitute(templates.fetchDocument, query.toExpression(schema.getDocumentFormatter()));
    }
    
    Query getNameQuery(Id rootId, QualifiedName nameWithPatterns) {
        Query query;
        if (nameWithPatterns.isEmpty()) 
            query = Query.from("id", Range.equals(Json.createValue(rootId.toString())));
        else if (nameWithPatterns.parent.isEmpty()) 
            query = Query.from("parentId", Range.equals(Json.createValue(rootId.toString()))).intersect(Query.from("name", Range.like(nameWithPatterns.part)));
        else query = Query.from("parent", getNameQuery(rootId, nameWithPatterns.parent)).intersect(Query.from("name", Range.like(nameWithPatterns.part)));
        return query.intersect(Query.from("deleted", Range.equals(JsonValue.FALSE)));
    }
    
    String searchDocumentLinkSQL(Id rootId, QualifiedName nameWithPatterns, Query filter) {
        filter = getDBFilterExpression(schema.getLinkFields(), filter).intersect(getNameQuery(rootId, nameWithPatterns));
        return Templates.substitute(templates.fetchDocumentLink, getNameExpression(nameWithPatterns.size()), filter.toExpression(schema.getLinkFormatter(false)));
    }
    
    String searchDocumentLinkSQL(Id rootId, QualifiedName nameWithPatterns, Id docId, Query filter) {
        Query nameQuery = Query.from("parent", getNameQuery(rootId, nameWithPatterns))
            .intersect(Query.from("id", Range.equals(Json.createValue(docId.toString()))));
        filter = getDBFilterExpression(schema.getLinkFields(), filter).intersect(nameQuery);
        return Templates.substitute(templates.fetchDocumentLink, getNameExpression(nameWithPatterns.size()+1), filter.toExpression(schema.getLinkFormatter(false)));
    }
    
    String searchDocumentLinkSQL(QualifiedName nameWithPatterns, Id docId, Query filter) {
        Query nameQuery = Query.from("parent", getNameQuery(nameWithPatterns))
            .intersect(Query.from("id", Range.equals(Json.createValue(docId.toString()))));
        filter = getDBFilterExpression(schema.getLinkFields(), filter).intersect(nameQuery);
        return Templates.substitute(templates.fetchDocumentLink, getNameExpression(nameWithPatterns.size()), filter.toExpression(schema.getLinkFormatter(false)));
    }

    String getFolderSQL(int depth) {
        return Templates.substitute(templates.fetchFolder, getNameExpression(depth), getParameterizedNameQuery(depth).toExpression(schema.getFolderFormatter()));
    }

    String searchFolderSQL(Id rootId, QualifiedName nameWithPatterns, Query filter) {
        filter = getDBFilterExpression(schema.getFolderFields(), filter).intersect(getNameQuery(rootId, nameWithPatterns));
        return Templates.substitute(templates.fetchFolder, getNameExpression(nameWithPatterns.size()), filter.toExpression(schema.getFolderFormatter()));
    }

    String getDocumentLinkByIdSQL(int depth, Optional<String> version) {
        Query query;
        if (depth == 0) 
            query = Query.from("parentId", Range.equals(Param.from("0"))).intersect(Query.from("id", Range.equals(Param.from("1"))));
        else 
            query = Query.from("parent", getParameterizedNameQuery(depth)).intersect(Query.from("id", Range.equals(Param.from(Integer.toString(depth)))));
        if (version.isPresent()) {
            query = query.intersect(Query.from("version", Range.equals(Param.from(Integer.toString(depth+1)))));
        }
            
        return Templates.substitute(templates.fetchDocumentLink, getNameExpression(depth+1), query.toExpression(schema.getLinkFormatter(version.isPresent())));
    }

    public Optional<QualifiedName> getPathTo(Id id) throws SQLException {
        LOG.entry(id);
        if (id.equals(Id.ROOT_ID)) 
            return LOG.exit(Optional.of(QualifiedName.ROOT));
        else try (Stream<QualifiedName> results = FluentStatement
                .of(operations.fetchPathToId)
                .set(1, id)
                .execute(con, rs->QualifiedName.parse(rs.getString(1),"/"))) {
            return LOG.exit(
                results.findFirst()
            );
        }
    }
    
    public String generateUniqueName(Id id, final String nameTemplate) throws SQLException {
		int separator = nameTemplate.lastIndexOf('.');
        String ext = "";
        String baseName = nameTemplate;
		if (separator >= 0) {
			ext = baseName.substring(separator, baseName.length());
			baseName = baseName.substring(0, separator);
		} 

        Optional<String> match = Optional.empty();
        try (Stream<String> matches = FluentStatement
            .of(operations.fetchLastNameLike)
            .set(1, id)
            .set(2, baseName+"%"+ext)
            .execute(con, rs->{ 
                String name = rs.getString(1); 
                return name == null ? "" : name;
            })
        ) {
            match = matches.findFirst();
        }
       
        if (match.isPresent() && match.get().length() > 0) {
            String prev = match.get();
            int extIndex = prev.lastIndexOf(ext);
            if (extIndex < 0) extIndex = prev.length();
            String postfix = prev.substring(baseName.length(), extIndex);
            if (postfix.length() > 0) {
                int lastChar = postfix.charAt(postfix.length()-1);
                int charIndex = SAFE_CHARACTERS.indexOf(lastChar);
                if (charIndex < 0 || lastChar == MAX_SAFE_CHAR) {
                    postfix = postfix + SAFE_CHARACTERS.charAt(0);
                } else {
                    postfix = postfix.substring(0, postfix.length() - 1) + SAFE_CHARACTERS.charAt(charIndex + 1);
                }
            } else {
                postfix = "_1";
            }
            return baseName + postfix + ext;

        } else {
            return nameTemplate;
        }
    }
    
    public void createDocument(Id id, Id version, String mediaType, long length, byte[] digest, JsonObject metadata) throws SQLException {
        LOG.entry(mediaType, length, digest, metadata);
        if (metadata == null) metadata = JsonObject.EMPTY_JSON_OBJECT;
        FluentStatement.of(operations.createVersion)
            .set(1, id)
            .set(2, version)
            .set(3, mediaType)
            .set(4, length)
            .set(5, digest)
            .set(6, clobWriter(metadata))
            .execute(con);             
        FluentStatement.of(operations.createDocument)
            .set(1, id)
            .set(2, version)
            .execute(con);  
        LOG.exit();        
    }
    
    public void createVersion(Id id, Id version, String mediaType, long length, byte[] digest, JsonObject metadata) throws SQLException {
        LOG.entry(mediaType, length, digest, metadata);
        FluentStatement.of(operations.createVersion)
            .set(1, id)
            .set(2, version)
            .set(3, mediaType)
            .set(4, length)
            .set(5, digest)
            .set(6, clobWriter(metadata))
            .execute(con);             
        FluentStatement.of(operations.updateDocument)
            .set(2, id)
            .set(1, version)
            .execute(con);  
        LOG.exit();        
    }

    public <T> Optional<T> getDocument(Id id, Id version, Mapper<T> mapper) throws SQLException {
        LOG.entry(id, version , mapper);
        if (version == null) {
            try (Stream<T> result = FluentStatement.of(operations.fetchLatestDocument).set(1, id).execute(con, mapper)) {
                return LOG.exit(result.findFirst());
            }
        } else {
            try (Stream<T> result = FluentStatement.of(operations.fetchDocument).set(1, id).set(2, version).execute(con, mapper)) {
                return LOG.exit(result.findFirst());
            }
        }
    }
    
    public <T> Stream<T> getDocuments(Id id, Query query, Mapper<T> mapper) throws SQLException {
        LOG.entry(id, query, mapper);
        Stream<T> result = FluentStatement.of(getDocumentSearchHistorySQL(query)).set(1, id).execute(schema.datasource, mapper);
        return LOG.exit(result);
    }
    
    public <T> Stream<T> getDocuments(Query query, boolean searchHistory, Mapper<T> mapper) throws SQLException {
        LOG.entry(query, searchHistory, mapper);
        Stream<T> result = FluentStatement.of(getDocumentSearchSQL(query, searchHistory)).execute(schema.datasource, mapper);
        return LOG.exit(result);
    }
    
    public <T> T createFolder(Id parentId, String name, Workspace.State state, JsonObject metadata, Mapper<T> mapper) throws SQLException {
        LOG.entry(parentId, name, state, metadata);
        Id id = new Id();
        FluentStatement.of(operations.createNode)
            .set(1, id)
            .set(2, parentId)
            .set(3, name)
            .set(4, RepositoryObject.Type.WORKSPACE.toString())
            .execute(con);
        FluentStatement.of(operations.createFolder)
            .set(1, id)
            .set(2, state.toString())
            .set(3, clobWriter(metadata))
            .execute(con);
        
        
        try (Stream<T> result = FluentStatement.of(this.getFolderSQL(1))
            .set(1, name)
            .set(2, parentId)
            .execute(con, mapper)
        ) {
            return LOG.exit(
                result.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results"))
            );
        }
    }

    public <T> Optional<T> getFolder(Id parentId, QualifiedName name, Mapper<T> mapper) throws SQLException {
        LOG.entry(parentId, name);
        if (name.isEmpty()) {
            try (Stream<T> result = FluentStatement.of(getFolderSQL(0))
                .set(1, parentId)
                .execute(con, mapper)
            ) { 
                return LOG.exit(result.findFirst());
            }            
        } else {
            try (Stream<T> result = FluentStatement.of(getFolderSQL(name.size()))
                .set(1,name)
                .set(name.size()+1,parentId)
                .execute(con, mapper)
            ) { 
                return LOG.exit(result.findFirst());
            }
        }
    }
    
    public <T> Stream<T> getFolders(Id rootId, QualifiedName path, Query filter, Mapper<T> mapper) throws SQLException {
        LOG.entry(rootId, path, mapper);
        return LOG.exit(FluentStatement
            .of(searchFolderSQL(rootId, path, filter))
            .execute(schema.datasource, mapper)
        );
    }
        
    public <T> Optional<T> getInfo(Id rootId, QualifiedName name, Mapper<T> mapper) throws SQLException {
        LOG.entry(rootId, name);
        try (Stream<T> results = FluentStatement.of(getInfoSQL(name.size()))
            .set(1, name)
            .set(name.size()+1, rootId)
            .execute(con, mapper)) {
            return LOG.exit(results.findFirst());
        }
    }
    
    public Stream<Info> getChildren(Id parentId) throws SQLException {
        LOG.entry(parentId);
        return LOG.exit(FluentStatement.of(operations.fetchChildren)
            .set(1, parentId)
            .execute(con, GET_INFO)
        );        
    }
    
    public <T> Optional<T> getOrCreateFolder(Id parentId, String name, boolean optCreate, Mapper<T> mapper) throws SQLException, InvalidWorkspace {
        Optional<T> folder = getFolder(parentId, QualifiedName.of(name), mapper);
        if (!folder.isPresent() && optCreate)
            folder = Optional.of(createFolder(parentId, name, Workspace.State.Open, JsonObject.EMPTY_JSON_OBJECT, mapper));
        return folder;
    }
    

    
    public <T> Optional<T> getOrCreateFolder(Id rootId, QualifiedName path, boolean optCreate, Mapper<T> mapper) throws InvalidWorkspace, SQLException {
        LOG.entry(rootId, path, optCreate, mapper);
        Optional<Id> parentId = path.parent.isEmpty() 
            ? Optional.of(rootId) 
            : getOrCreateFolder(rootId, path.parent, optCreate, GET_ID);
        return parentId.isPresent() 
            ? getOrCreateFolder(parentId.get(), path.part, optCreate, mapper)
            : Optional.empty();
    }
    
    public <T> T copyFolder(Id sourceId, QualifiedName sourcePath, Id targetId, QualifiedName targetPath, boolean optCreate, Mapper<T> mapper) throws SQLException, InvalidObjectName, InvalidWorkspace {
        LOG.entry(sourceId, sourcePath, targetId, targetPath, optCreate, mapper);
        Id idSrc = getFolder(sourceId, sourcePath, GET_ID).orElseThrow(()->new InvalidWorkspace(sourceId.toString(), sourcePath));
        Id folderId = targetPath.parent.isEmpty() 
            ? targetId
            : getOrCreateFolder(targetId, targetPath.parent, optCreate, GET_ID)
                .orElseThrow(()->new InvalidWorkspace(targetId.toString(), targetPath.parent));
            
        Id id = new Id();
        FluentStatement.of(operations.createNode)
            .set(1, id)
            .set(2, folderId)
            .set(3, targetPath.part)
            .set(4, RepositoryObject.Type.WORKSPACE.toString())
            .execute(con);
        FluentStatement.of(operations.copyFolder)
            .set(1, id)
            .set(2, idSrc)
            .execute(con);
        
        Iterable<Info> children = FluentStatement.of(operations.fetchChildren)
            .set(1, idSrc)
            .execute(con, GET_INFO)
            .collect(Collectors.toList());
        for (Info child : children) {
            switch(child.type) {
                case WORKSPACE:
                    copyFolder(idSrc, child.path, id, child.path, false, GET_ID);
                    break;
                case DOCUMENT_LINK:
                    copyDocumentLink(idSrc, child.path, id, child.path, false, GET_ID);
                    break;
                default:
                    throw new RuntimeException("don't know how to copy " + child.type);
            }
        }
        
        try (Stream<T> results = FluentStatement.of(getFolderSQL(1))
            .set(1, targetPath.part)
            .set(2, folderId)
            .execute(con, mapper)) {
        
            return LOG.exit(
                results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results")));
        }
    }
    
    public <T> T copyDocumentLink(Id sourceId, QualifiedName sourcePath, Id targetId, QualifiedName targetPath, boolean optCreate, Mapper<T> mapper) throws SQLException, InvalidObjectName, InvalidWorkspace {
        LOG.entry(sourceId, sourcePath, targetId, targetPath, optCreate, mapper);
        Id idSrc = getDocumentLink(sourceId, sourcePath, Optional.empty(), GET_ID).orElseThrow(()->new InvalidObjectName(sourceId.toString(), sourcePath));
        Id folderId = targetPath.parent.isEmpty() 
            ? targetId
            : getOrCreateFolder(targetId, targetPath.parent, optCreate, GET_ID)
                .orElseThrow(()->new InvalidWorkspace(targetId.toString(), targetPath.parent));
        Id id = new Id();
        FluentStatement.of(operations.createNode)
            .set(1, id)
            .set(2, folderId)
            .set(3, targetPath.part)
            .set(4, RepositoryObject.Type.DOCUMENT_LINK.toString())
            .execute(con);
        FluentStatement.of(operations.copyLink)
            .set(1, id)
            .set(2, idSrc)
            .execute(con);
        
        try (Stream<T> results = LOG.exit(FluentStatement.of(getDocumentLinkSQL(1, Optional.empty()))
            .set(1, targetPath.part)
            .set(2, folderId)
            .execute(con, mapper))) {       
        
            return results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results"));
        }
    }
    
    
    public <T> T createDocumentLink(Id folderId, String name, Id docId, Id version, Mapper<T> mapper) throws SQLException {
        LOG.entry(folderId, name, docId, version);
        Id id = new Id();
        FluentStatement.of(operations.createNode)
            .set(1, id)
            .set(2, folderId)
            .set(3, name)
            .set(4, RepositoryObject.Type.DOCUMENT_LINK.toString())
            .execute(con);
        FluentStatement.of(operations.createLink)
            .set(1, id)
            .set(2, docId)
            .set(3, version)
            .set(4, false)
            .execute(con);
        
        try (Stream<T> results = FluentStatement.of(getDocumentLinkSQL(1, Optional.empty()))
            .set(1, name)
            .set(2, folderId)
            .execute(con, mapper)) { 
        return LOG.exit(
            results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results")));
        }
    }
       
    public <T> Optional<T> updateDocumentLink(Id folderId, String name, Id docId, Id version, Mapper<T> mapper) throws InvalidObjectName, SQLException {
        LOG.entry(folderId, name, docId, version);
        Optional<Info> info = getInfo(folderId, QualifiedName.of(name), GET_INFO);
        if (info.isPresent()) {
            FluentStatement.of(operations.updateLink)
                .set(1, docId)
                .set(2, version)
                .set(3, info.get().id)
                .execute(con);
            try (Stream<T> results = FluentStatement.of(getDocumentLinkSQL(1, Optional.empty()))
                .set(1, name)
                .set(2, folderId)
                .execute(con, mapper)
            ) {
                return LOG.exit(
                    results.findFirst()
                );
            }
        } else {
            return Optional.empty();
        }
    }
    
    public <T> Optional<T> updateFolder(Id folderId, Workspace.State state, JsonObject metadata, Mapper<T> mapper) throws SQLException {
        LOG.entry(folderId, state, metadata);
        int count = FluentStatement.of(operations.updateFolder)
            .set(1, state.toString())
            .set(2, metadata)
            .set(3, folderId)
            .execute(con);
        if (count == 0) return Optional.empty();
        try (Stream<T> result = FluentStatement.of(getFolderSQL(0))
            .set(1, folderId)
            .execute(con, mapper)
        ) {
            return LOG.exit(
                result.findFirst()
            );
        }
    }
    
    public void lockVersions(Id folderId) throws SQLException {
        LOG.entry(folderId);
        FluentStatement.of(operations.lockVersions)
            .set(1, folderId)
            .execute(con);
    }
    
    public void unlockVersions(Id folderId) throws SQLException {
        LOG.entry(folderId);
        FluentStatement.of(operations.unlockVersions)
            .set(1, folderId)
            .execute(con);
    }

    public <T> Optional<T> getDocumentLink(Id rootId, QualifiedName workspacePath, Id documentId, Optional<String> version, Mapper<T> mapper) throws SQLException {
        LOG.entry(rootId, workspacePath, documentId, mapper);
        try (Stream<T> result = FluentStatement
            .of(getDocumentLinkByIdSQL(workspacePath.size(), version))
            .set(1, documentId)
            .set(2, workspacePath)
            .set(workspacePath.size()+2, rootId)
            .set(workspacePath.size()+3, version)
            .execute(con, mapper)
        ) { 
            return LOG.exit(result.findFirst());
        }
    }
    
    public <T> Optional<T> getDocumentLink(Id rootId, QualifiedName path, Optional<String> version, Mapper<T> mapper) throws SQLException {
        LOG.entry(rootId, path, mapper);
        try (Stream<T> result = FluentStatement
            .of(getDocumentLinkSQL(path.size(), version))
            .set(1, path)
            .set(path.size()+1, rootId)
            .set(path.size()+2, version)
            .execute(con, mapper)
        ) {
            return LOG.exit(result.findFirst());
        }
    }
    
    public <T> Stream<T> getDocumentLinks(Id rootId, QualifiedName path, Query filter, Mapper<T> mapper) throws SQLException {
        LOG.entry(rootId, path, filter, mapper);
        return LOG.exit(FluentStatement
            .of(searchDocumentLinkSQL(rootId, path, filter))
            .execute(schema.datasource, mapper)
        );
    }
    
    public <T> Stream<T> getDocumentLinks(Id rootId, QualifiedName workspacePath, Id docId, Query filter, Mapper<T> mapper) throws SQLException {
        LOG.entry(rootId, workspacePath, filter, mapper);
        return LOG.exit(FluentStatement
            .of(searchDocumentLinkSQL(rootId, workspacePath, docId, filter))
            .execute(schema.datasource, mapper)
        );
    }

    public <T> Stream<T> getDocumentLinks(QualifiedName workspacePath, Id docId, Query filter, Mapper<T> mapper) throws SQLException {
        LOG.entry(workspacePath, filter, mapper);
        return LOG.exit(FluentStatement
            .of(searchDocumentLinkSQL(workspacePath, docId, filter))
            .execute(schema.datasource, mapper)
        );
    }
    public void deleteObject(Id rootId, QualifiedName path) throws SQLException, InvalidObjectName {
        LOG.entry(rootId, path);
        Id objectId = getInfo(rootId, path, GET_ID).orElseThrow(()->new InvalidObjectName(rootId.toString(), path));
        FluentStatement.of(operations.deleteObject).set(1, objectId).execute(con);
    }
    
    public void deleteDocumentById(Id rootId, QualifiedName workspacePath, Id documentId) throws SQLException, InvalidWorkspace, InvalidDocumentId {
        LOG.entry(rootId, workspacePath, documentId);
        Id parentId = getInfo(rootId, workspacePath, GET_ID).orElseThrow(()->new InvalidWorkspace(rootId.toString(), workspacePath));
        int count = FluentStatement.of(operations.deleteDocumentById).set(1, parentId).set(2, documentId).execute(con);
        if (count == 0) throw new Exceptions.InvalidDocumentId(documentId.toString());
    }
    
    public SQLAPI(DataSource ds) throws SQLException {
        con = ds.getConnection();
        con.setAutoCommit(false);
    }
    
    @Override
    public void close() throws SQLException {
        con.rollback();
        con.close();
    }
    
    public <T extends Throwable> void rollbackOrThrow(Supplier<T> supplier) throws T {
        try {
            con.rollback();
        } catch (SQLException e) {
            LOG.catching(e);
            throw(supplier.get());
        }
    }
    
    public <T extends Throwable> void commitOrThrow(Supplier<T> supplier) throws T {
        try {
            con.commit();
        } catch (SQLException e) {
            LOG.catching(e);
            throw(supplier.get());
        }
    }
    
    public void commit() throws SQLException {
        con.commit();
    }
}
