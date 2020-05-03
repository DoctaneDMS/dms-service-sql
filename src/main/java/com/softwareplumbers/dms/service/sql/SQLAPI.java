/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.abstractquery.Param;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.common.abstractquery.Range;
import com.softwareplumbers.common.immutablelist.QualifiedName;
import com.softwareplumbers.common.jsonview.JsonViewFactory;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Exceptions.InvalidDocumentId;
import com.softwareplumbers.dms.Exceptions.InvalidObjectName;
import com.softwareplumbers.dms.Exceptions.InvalidWorkspace;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryObject;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.RepositoryPath.DocumentIdElement;
import com.softwareplumbers.dms.RepositoryPath.ElementType;
import com.softwareplumbers.dms.RepositoryPath.IdElement;
import com.softwareplumbers.dms.RepositoryPath.Versioned;
import com.softwareplumbers.dms.RepositoryPath.VersionedElement;
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
import com.softwareplumbers.common.abstractquery.visitor.Visitors.SQLResult;
import com.softwareplumbers.dms.Constants;
import com.softwareplumbers.dms.Exceptions.InvalidWorkspaceState;
import com.softwareplumbers.dms.RepositoryPath.Element;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


/**
 *
 * @author jonathan
 */
public class SQLAPI implements AutoCloseable {
    
    private static XLogger LOG = XLoggerFactory.getXLogger(SQLAPI.class);
    
    private static final String SAFE_CHARACTERS ="0123456789ABCDEFGHIJKLMNOPQURSTUVWYXabcdefghijklmnopqrstuvwxyz";
    private static final int MAX_SAFE_CHAR='z';
    private static final byte[] ROOT_ID = new byte[] { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };
    private static final Range NULL_VERSION = Range.equals(Json.createValue(""));
    private static final RepositoryPath ROOT_PATH = RepositoryPath.ROOT.addId(Id.ROOT_ID.toString());
    private static final Workspace ROOT_WORKSPACE = new WorkspaceImpl(Id.ROOT_ID.toString(), null, RepositoryPath.ROOT, Workspace.State.Open, Constants.EMPTY_METADATA, true, LocalData.NONE);

    public static JsonObject toJson(Reader reader) {
        try (JsonReader jsonReader = Json.createReader(reader)) {
            return jsonReader.readObject();
        }
    }
    
    public static final Mapper<Id> GET_ID = results -> {
        return new Id(results.getBytes("ID"));
    };

    public static final Mapper<Id> GET_VERIFIED_ID = results -> {
        return new Id(results.getBytes("ID"));
    };
    
    public static final Mapper<String> GET_NAME = results -> {
        return results.getString("NAME");
    };

    public static final Mapper<Document> GET_DOCUMENT = results -> {
        String mediaType = results.getString("MEDIA_TYPE");
        Id id = new Id(results.getBytes("DOCUMENT_ID"));
        Id version = new Id(results.getBytes("VERSION_ID"));
        long length = results.getLong("LENGTH");
        byte[] hash = results.getBytes("DIGEST");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        return new DocumentImpl(new Reference(id.toString(),version.toString()), mediaType, length, hash, metadata, false, LocalData.NONE);
    };
    
    public static final Mapper<Reference> GET_REFERENCE = results -> {
        Id id = new Id(results.getBytes("DOCUMENT_ID"));
        Id version = new Id(results.getBytes("VERSION_ID"));
        return new Reference(id.toString(),version.toString());        
    };
    
    public Optional<RepositoryPath> getBasePath(Id id) throws SQLException {
        try (Stream<RepositoryPath> names = FluentStatement
            .of(operations.fetchPathToId)
            .set(1, id)
            .execute(con, rs->RepositoryPath.valueOf(rs.getString(1)))
        ) {

            return names.findFirst();
        }                  
    }
    
    public static DocumentLink getLink(ResultSet results, RepositoryPath basePath) throws SQLException {
        String mediaType = results.getString("MEDIA_TYPE");
        Id docId = new Id(results.getBytes("DOCUMENT_ID"));
        String docVersion = results.getString("VERSION_ID");
        long length = results.getLong("LENGTH");
        byte[] hash = results.getBytes("DIGEST");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        Id id = new Id(results.getBytes("ID"));
        String version = results.getString("VERSION");
        
        Optional<IdElement> rootId = basePath.getId();
                
        RepositoryPath name = basePath.addAll(RepositoryPath.valueOf(results.getString("PATH")));
        return new DocumentLinkImpl(id.toString(), version, name, new Reference(docId.toString(),docVersion), mediaType, length, hash, metadata, false, LocalData.NONE);
    }
    
    public static Mapper<DocumentLink> GET_LINK = rs -> getLink(rs, RepositoryPath.ROOT);
    
    public static Consumer<Writer> clobWriter(JsonObject metadata) {
        return out -> { 
            try (JsonWriter writer = Json.createWriter(out)) { 
                writer.write(metadata); 
            } 
        };
    }
    

    public static Mapper<Workspace> GET_WORKSPACE = results -> {
        Id id = new Id(results.getBytes("ID"));
        String version = results.getString("VERSION");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        Workspace.State state = Workspace.State.valueOf(results.getString("STATE"));
        RepositoryPath path = RepositoryPath.valueOf(results.getString("PATH"));
        return new WorkspaceImpl(id.toString(), version, path, state, metadata, false, LocalData.NONE);
    };

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
        Id parent_id = Id.of(results.getBytes("PARENT_ID"));
        String name = results.getString("NAME");
        RepositoryObject.Type type = RepositoryObject.Type.valueOf(results.getString("TYPE"));
        RepositoryPath path = RepositoryPath.valueOf(results.getString("PATH"));
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
    
    Query getNameQuery(RepositoryPath name, boolean hideDeleted, boolean implicitRoot) {
        
        if (name.isEmpty()) return Query.UNBOUNDED;
        
        Query result;
        
        if (name.parent.isEmpty()) {
            if (name.part.type != RepositoryPath.ElementType.OBJECT_ID && implicitRoot) {
                result = Query.from("parentId", Range.equals(Json.createValue(Id.ROOT_ID.toString())));              
            } else {
                result = Query.UNBOUNDED;
            }
        } else {        
            // First get the query for the parent part of the name
            if (name.parent.part.type == RepositoryPath.ElementType.OBJECT_ID) {
                // this shortcut basically just avoids joining to the parent node if the criteria
                // is just on the node id
                result = Query.from("parentId", Range.like(((IdElement)name.parent.part).id));
            } else {
                result = Query.from("parent", getNameQuery(name.parent, hideDeleted, implicitRoot));
            }
        }
        
        // Filter out anything that has been deleted
        if (hideDeleted) result = result.intersect(Query.from("deleted", Range.equals(JsonValue.FALSE)));
        
        // Now add the query for this part of the name
        switch (name.part.type) {
            case DOCUMENT_PATH:
                VersionedElement pathElement = (VersionedElement)name.part;
                result = result.intersect(Query.from("name", Range.like(pathElement.name)));
                result = result.intersect(Query.from("version", pathElement.version == null ? NULL_VERSION : Range.equals(Json.createValue(pathElement.version))));                
                break;
            case DOCUMENT_ID:
                DocumentIdElement docIdElement = (DocumentIdElement)name.part;
                result = result.intersect(Query.from("reference", Query.from("id", Range.equals(Json.createValue(docIdElement.id)))));
                result = result.intersect(Query.from("version", docIdElement.version == null ? NULL_VERSION : Range.equals(Json.createValue(docIdElement.version))));                
                break;
            case OBJECT_ID:
                IdElement idElement = (IdElement)name.part;
                result = result.intersect(Query.from("id", Range.like(idElement.id)));
                break;
            default:
                throw new RuntimeException("Unsupported element type in path " + name);
        }
        return result;
    } 
    
    Query getParameterizedNameQuery(String paramName, RepositoryPath name) {
        
        if (name.isEmpty()) return Query.from("id", Range.equals(Param.from(paramName)));
        
        Query result = Query.UNBOUNDED;
                                
        // Now add the query for this part of the name
        switch (name.part.type) {
            case DOCUMENT_PATH:
                result = result.intersect(Query.from("name", Range.equals(Param.from(paramName))));
                result = result.intersect(Query.from("version", Range.equals(Param.from(paramName+".version"))));                
                break;
            case DOCUMENT_ID:
                result = result.intersect(Query.from("reference", Query.from("id", Range.equals(Param.from(paramName)))));
                result = result.intersect(Query.from("version", Range.equals(Param.from(paramName+".version"))));                
                break;
            case OBJECT_ID:
                result = result.intersect(Query.from("id", Range.equals(Param.from(paramName))));
                break;
            default:
                throw new RuntimeException("Unsupported element type in path " + name);
        }        

        if (name.parent.isEmpty()) {
            if (name.part.type != RepositoryPath.ElementType.OBJECT_ID) {
                result = Query
                    .from("parentId", Range.equals(Param.from("parent." + paramName)))
                    .intersect(result);              
            } 
        } else {        
            // First get the query for the parent part of the name
            if (name.parent.part.type == RepositoryPath.ElementType.OBJECT_ID) {
                // this shortcut basically just avoids joining to the parent node if the criteria
                // is just on the node id
                result = Query
                    .from("parentId", Range.equals(Param.from("parent." + paramName)))
                    .intersect(result);
            } else {
                result = Query
                    .from("parent", getParameterizedNameQuery("parent." + paramName, name.parent))
                    .intersect(result);
            }
        }
                
        return result;        
    }
    
    
    String getNameExpression(RepositoryPath basePath, RepositoryPath path) {
        StringBuilder builder = new StringBuilder();
        int depth = path.afterRootId().size();
        builder.append("'").append(basePath.join("/")).append("'");      
        for (int i = depth - 1; i >= 0 ; i--)
            builder.append(Templates.substitute(templates.nameExpr, i));
        return builder.toString();
    }
    
    SQLResult getParametrizedNameExpression(RepositoryPath path) {
        StringBuilder builder = new StringBuilder();
        int depth = path.afterRootId().size();
        builder.append("?");      
        for (int i = depth - 1; i >= 0 ; i--)
            builder.append(Templates.substitute(templates.nameExpr, i));
        return new SQLResult(builder.toString(), Collections.singletonList("basePath"));
    }
    
    Query getDBFilterExpression(Iterable<QualifiedName> validFields, Query filter) {
        return StreamSupport.stream(validFields.spliterator(), false).reduce(Query.UNBOUNDED, (query, name) -> query.intersect(filter.getConstraint(name)), (query1, query2)->query1.intersect(query2));
    }
    
    private <T> List<T> concat(Collection<T>... iterables) {
        return Stream.of(iterables).flatMap(Collection::stream).collect(Collectors.toList());
    }
    
    SQLResult getInfoSQL(RepositoryPath path) {
        int depth = path.getDocumentPath().size();
        SQLResult criteria = path.getDocumentId().isPresent()  
            ? getParameterizedNameQuery("path", path).toExpression(schema.getLinkFormatter(false))
            : getParameterizedNameQuery("path", path).toExpression(schema.getNodeFormatter());
        SQLResult name =  getParametrizedNameExpression(path);
        String sql = Templates.substitute(templates.fetchInfo, name.sql, criteria.sql);
        return new SQLResult(sql, concat(name.parameters, criteria.parameters));
    }
    
    SQLResult getDocumentLinkSQL(RepositoryPath path) {
        Query query = getParameterizedNameQuery("path", path);
        int depth = path.getDocumentPath().size();
        // This test is only needed 
        boolean versionRequested = path.part instanceof Versioned ? ((Versioned)path.part).getVersion() != null : false;
        SQLResult criteria = query.toExpression(schema.getLinkFormatter(versionRequested));
        SQLResult name =  getParametrizedNameExpression(path);
        String sql = Templates.substitute(templates.fetchDocumentLink, name.sql, criteria.sql);
        List<String> parameters = concat(name.parameters, criteria.parameters);
        return new SQLResult(sql, parameters);
    }
    
    String getDocumentSearchSQL(Query query, boolean searchHistory) {
        query = getDBFilterExpression(schema.getDocumentFields(), query);
        if (!searchHistory) query = query.intersect(Query.from("latest", Range.equals(JsonValue.TRUE)));
        return Templates.substitute(templates.fetchDocument, query.toExpression(schema.getDocumentFormatter()).sql);
    }
    
    String getDocumentSearchHistorySQL(Query query) {
        query = getDBFilterExpression(schema.getDocumentFields(), query);
        query = query.intersect(Query.from(QualifiedName.of("reference","id"), Range.equals(Param.from("0"))));
        return Templates.substitute(templates.fetchDocument, query.toExpression(schema.getDocumentFormatter()).sql);
    }
    

    
    String searchDocumentLinkSQL(RepositoryPath basePath, RepositoryPath nameWithPatterns, Query filter, boolean implicitRoot) {
        filter = getDBFilterExpression(schema.getLinkFields(), filter).intersect(getNameQuery(nameWithPatterns, true, implicitRoot));
        return Templates.substitute(templates.fetchDocumentLink, getNameExpression(basePath, nameWithPatterns), filter.toExpression(schema.getLinkFormatter(false)).sql);
    }
    
    SQLResult getFolderSQL(RepositoryPath path) {
        int depth = path.getDocumentPath().size();   
        SQLResult criteria = getParameterizedNameQuery("path", path).toExpression(schema.getFolderFormatter());
        SQLResult name = getParametrizedNameExpression(path);
        String sql = Templates.substitute(templates.fetchFolder, name.sql, criteria.sql);
        return new SQLResult(sql, concat(name.parameters, criteria.parameters));
    }

    String searchFolderSQL(RepositoryPath basePath, RepositoryPath nameWithPatterns, Query filter) {
        filter = getDBFilterExpression(schema.getFolderFields(), filter).intersect(getNameQuery(nameWithPatterns, true, true));
        return Templates.substitute(templates.fetchFolder, getNameExpression(basePath, nameWithPatterns), filter.toExpression(schema.getFolderFormatter()).sql);
    }

    public Optional<RepositoryPath> getPathTo(Id id) throws SQLException {
        LOG.entry(id);
        if (id.equals(Id.ROOT_ID)) 
            return LOG.exit(Optional.of(RepositoryPath.ROOT));
        else try (Stream<RepositoryPath> results = FluentStatement
                .of(operations.fetchPathToId)
                .set(1, id)
                .execute(con, rs->RepositoryPath.valueOf(rs.getString(1)))) {
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
    
    public <T> Optional<RepositoryPath> getBasePath(RepositoryPath path, Mapper<T> mapper) throws SQLException {
        if (mapper == GET_WORKSPACE || mapper == GET_LINK) {
            Optional<IdElement> idElement = path.getId();
            if (idElement.isPresent()) {
                Id id = Id.of(idElement.get().id);
                return getPathTo(id);
            }
        }
        return Optional.of(RepositoryPath.ROOT);
    }
    
    public <T> T createFolder(Id parentId, String name, Workspace.State state, JsonObject metadata, Mapper<T> mapper) throws SQLException, InvalidWorkspace {
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
        
        RepositoryPath path = RepositoryPath.ROOT.addId(parentId.toString()).addDocumentPath(name);
                
        try (Stream<T> result = FluentStatement.of(this.getFolderSQL(path))
            .set("basePath", getBasePath(path, mapper).orElseThrow(()->new InvalidWorkspace(path)).join("/"))    
            .set("path", path)
            .execute(con, mapper)
        ) {
            return LOG.exit(
                result.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results"))
            );
        }
    }

    public <T> Optional<T> getFolder(RepositoryPath name, Mapper<T> mapper) throws SQLException {
        LOG.entry(name);
        if (name.isEmpty() && (mapper == GET_ID || mapper == GET_VERIFIED_ID))
            return Optional.of((T)Id.ROOT_ID);
        if (!name.isEmpty() && name.part.type == ElementType.OBJECT_ID && mapper == GET_ID) {
            // If all we need is the ID (because the mapper id GET_ID) and we have an Id on the path,
            // don't bother calling the database to verify.
            IdElement pathId = (IdElement)name.part;
            return Optional.of((T)Id.of(pathId.id));
        } else {
            Optional<RepositoryPath> basePath = getBasePath(name, mapper);
            if (!basePath.isPresent()) return Optional.empty();
            try (Stream<T> result = FluentStatement.of(getFolderSQL(name))
                .set("basePath",basePath.get().join("/"))
                .set("path", name)
                .execute(con, mapper)
            ) { 
                return LOG.exit(result.findFirst());
            }
        }
    }
    
    public <T> Stream<T> getFolders(RepositoryPath path, Query filter, Mapper<T> mapper) throws InvalidWorkspace, SQLException {
        LOG.entry(path, mapper);
        Optional<RepositoryPath> basePath = getBasePath(path, mapper);
        if (!basePath.isPresent()) return Stream.empty();
        return LOG.exit(FluentStatement
            .of(searchFolderSQL(basePath.get(), path, filter))
            .execute(schema.datasource, mapper)
        );
    }
        
    public <T> Optional<T> getInfo(RepositoryPath name, Mapper<T> mapper) throws SQLException {
        LOG.entry(name);
        Optional<RepositoryPath> basePath = getBasePath(name, mapper);
        if (!basePath.isPresent()) return Optional.empty();        
        try (Stream<T> results = FluentStatement.of(getInfoSQL(name))
            .set(1, basePath.get().join("/"))
            .set("path", name)
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
        Optional<T> folder = getFolder(RepositoryPath.ROOT.addId(parentId.toString()).addDocumentPath(name), mapper);
        if (!folder.isPresent() && optCreate)
            folder = Optional.of(createFolder(parentId, name, Workspace.State.Open, JsonObject.EMPTY_JSON_OBJECT, mapper));
        return folder;
    }
    

    
    public <T> Optional<T> getOrCreateFolder(RepositoryPath path, boolean optCreate, Mapper<T> mapper) throws InvalidWorkspace, SQLException {
        LOG.entry(path, optCreate, mapper);
        
        if (path.isEmpty()) return getFolder(path, mapper);

        switch (path.part.type) {
            case OBJECT_ID:
                return getFolder(path, mapper); // We can't create a folder without a name
            case DOCUMENT_PATH:
                VersionedElement docPath = (VersionedElement)path.part;
                if (path.parent.isEmpty()) {
                    return getOrCreateFolder(Id.ROOT_ID, docPath.name, optCreate, mapper);
                } else {
                    Optional<Id> parentId = getOrCreateFolder(path.parent, optCreate, GET_ID);
                    if (parentId.isPresent()) {
                        return getOrCreateFolder(parentId.get(), docPath.name, optCreate, mapper);
                    } else {
                        throw new InvalidWorkspace(path.parent);
                    }
                }
            default:
                throw new InvalidWorkspace(path);                    
        }
        
    }
    
    public <T> T copyFolder(RepositoryPath sourcePath, RepositoryPath targetPath, boolean optCreate, Mapper<T> mapper) throws SQLException, InvalidObjectName, InvalidWorkspace {
        LOG.entry(sourcePath, targetPath, optCreate, mapper);
        Id idSrc = getFolder(sourcePath, GET_VERIFIED_ID)
            .orElseThrow(()->new InvalidWorkspace(sourcePath));
        Id folderId = getOrCreateFolder(targetPath.parent, optCreate, GET_ID)
            .orElseThrow(()->new InvalidWorkspace(targetPath.parent));
        
        if (targetPath.part.type != ElementType.DOCUMENT_PATH) throw new InvalidObjectName(targetPath);
        
        VersionedElement docPart = (VersionedElement)targetPath.part;
        
        Id id = new Id();
        FluentStatement.of(operations.createNode)
            .set(1, id)
            .set(2, folderId)
            .set(3, docPart.name)
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
            RepositoryPath srcPath = RepositoryPath.ROOT.addId(idSrc.toString()).addAll(child.path);
            RepositoryPath tgtPath = RepositoryPath.ROOT.addId(id.toString()).addAll(child.path);
            switch(child.type) {
                case WORKSPACE:
                    copyFolder(srcPath, tgtPath, false, GET_ID);
                    break;
                case DOCUMENT_LINK:
                    copyDocumentLink(srcPath, tgtPath, false, GET_ID);
                    break;
                default:
                    throw new RuntimeException("don't know how to copy " + child.type);
            }
        }
        RepositoryPath resultPath = RepositoryPath.ROOT.addId(folderId.toString()).add(docPart);
        try (Stream<T> results = FluentStatement.of(getFolderSQL(resultPath))
            .set(1, getBasePath(resultPath, mapper).orElseThrow(()->new InvalidWorkspace(resultPath)).join("/"))
            .set("path", resultPath)
            .execute(con, mapper)
        ) {        
            return LOG.exit(
                results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results")));
        }
    }
    
    public <T> T copyDocumentLink(RepositoryPath sourcePath, RepositoryPath targetPath, boolean optCreate, Mapper<T> mapper) throws SQLException, InvalidObjectName, InvalidWorkspace {
        LOG.entry(sourcePath, targetPath, optCreate, mapper);
        Id idSrc = getDocumentLink(sourcePath, GET_ID)
            .orElseThrow(()->new InvalidObjectName(sourcePath));
        Id folderId = getOrCreateFolder(targetPath.parent, optCreate, GET_ID)
            .orElseThrow(()->new InvalidWorkspace(targetPath.parent));
        Id id = new Id();
        
        if (targetPath.part.type != ElementType.DOCUMENT_PATH) throw new InvalidObjectName(targetPath);
        VersionedElement linkName = (VersionedElement)targetPath.part;
        
        FluentStatement.of(operations.createNode)
            .set(1, id)
            .set(2, folderId)
            .set(3, linkName.name)
            .set(4, RepositoryObject.Type.DOCUMENT_LINK.toString())
            .execute(con);
        FluentStatement.of(operations.copyLink)
            .set(1, id)
            .set(2, idSrc)
            .execute(con);
        
        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString()).add(linkName);
        
        try (Stream<T> results = LOG.exit(FluentStatement.of(getDocumentLinkSQL(shortResultPath))
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->new InvalidWorkspace(shortResultPath)).join("/"))
            .set("path", shortResultPath)
            .execute(con, mapper))) {       
        
            return results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results"));
        }
    }
    
    public void copy(Id nodeId, Id newParentId) throws SQLException {
        LOG.entry(nodeId, newParentId);
        Id newId = new Id();
        FluentStatement.of(operations.copyNode)
            .set(1, newId)
            .set(2, newParentId)
            .set(3, nodeId)
            .execute(con); 
        FluentStatement.of(operations.copyLink)
            .set(1, newId)
            .set(2, nodeId)
            .execute(con);
        FluentStatement.of(operations.copyFolder)
            .set(1, newId)
            .set(2, nodeId)
            .execute(con);
        Iterable<Id> children = FluentStatement.of(operations.fetchChildren)
            .set(1, nodeId)
            .execute(con, GET_ID)
            .collect(Collectors.toList());
        for (Id child : children) {
            copy(child, newId);
        }
        LOG.exit();
    }
    
    public void publishChild(Id nodeId, Id newParentId) throws SQLException {
        LOG.entry(nodeId, newParentId);
        Id newId = new Id();
        FluentStatement.of(operations.copyNode)
            .set(1, newId)
            .set(2, newParentId)
            .set(3, nodeId)
            .execute(con); 
        FluentStatement.of(operations.publishLink)
            .set(1, newId)
            .set(2, nodeId)
            .execute(con);
        FluentStatement.of(operations.copyFolder)
            .set(1, newId)
            .set(2, nodeId)
            .execute(con);
        Iterable<Id> children = FluentStatement.of(operations.fetchChildren)
            .set(1, nodeId)
            .execute(con, GET_ID)
            .collect(Collectors.toList());
        for (Id child : children) {
            copy(child, newId);
        }
        LOG.exit();
    }
    
    public Id publish(Id nodeId, String version) throws SQLException {
        LOG.entry(nodeId, version);
        Id newId = new Id();
        FluentStatement.of(operations.publishNode)
            .set(1, newId)
            .set(2, version)
            .set(3, nodeId)
            .execute(con); 
        int links = FluentStatement.of(operations.publishLink)
            .set(1, newId)
            .set(2, nodeId)
            .execute(con);
        int folders = FluentStatement.of(operations.copyFolder)
            .set(1, newId)
            .set(2, nodeId)
            .execute(con);
        
        Iterable<Id> children = FluentStatement.of(operations.fetchChildren)
            .set(1, nodeId)
            .execute(con, GET_ID)
            .collect(Collectors.toList());
        for (Id child : children) {
            publishChild(child, newId);
        }
        
        return newId;
    }
        
    public <T> T createDocumentLink(Id folderId, String name, Id docId, Id version, Mapper<T> mapper) throws SQLException, InvalidWorkspace {
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
        
        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString()).addDocumentPath(name);
        
        try (Stream<T> results = FluentStatement.of(getDocumentLinkSQL(shortResultPath))
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->new InvalidWorkspace(shortResultPath)).join("/"))
            .set("path", shortResultPath)
            .execute(con, mapper)) { 
        return LOG.exit(
            results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results")));
        }
    }
       
    public <T> Optional<T> updateDocumentLink(Id folderId, String name, Id docId, Id version, Mapper<T> mapper) throws InvalidWorkspace, InvalidObjectName, SQLException {
        LOG.entry(folderId, name, docId, version);
        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString()).addDocumentPath(name);
        Optional<Info> info = getInfo(shortResultPath, GET_INFO);
        if (info.isPresent()) {
            FluentStatement.of(operations.updateLink)
                .set(1, docId)
                .set(2, version)
                .set(3, info.get().id)
                .execute(con);
                        
            try (Stream<T> results = FluentStatement.of(getDocumentLinkSQL(shortResultPath))
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->new InvalidWorkspace(shortResultPath)).join("/"))
            .set("path", shortResultPath)
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
    
    public <T> Optional<T> updateFolder(Id folderId, Workspace.State state, JsonObject metadata, Mapper<T> mapper) throws InvalidWorkspace, SQLException {
        LOG.entry(folderId, state, metadata);
        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString());
        int count = FluentStatement.of(operations.updateFolder)
            .set(1, state.toString())
            .set(2, metadata)
            .set(3, folderId)
            .execute(con);
        if (count == 0) return Optional.empty();
        try (Stream<T> result = FluentStatement.of(getFolderSQL(shortResultPath))
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->new InvalidWorkspace(shortResultPath)).join("/"))
            .set("path", shortResultPath)
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

    public <T> Optional<T> getDocumentLink(RepositoryPath path, Mapper<T> mapper) throws SQLException {
        LOG.entry(path, mapper);
        Optional<RepositoryPath> basePath = getBasePath(path, mapper);
        if (!basePath.isPresent()) return Optional.empty();
        try (Stream<T> result = FluentStatement
            .of(getDocumentLinkSQL(path))
            .set(1, basePath.get().join("/"))
            .set("path", path)
            .execute(con, mapper)
        ) {
            return LOG.exit(result.findFirst());
        }
    }
    
    public DocumentLink getFullPath(DocumentLink link) {
        try {
            RepositoryPath path = getPathTo(Id.of(link.getId()))
                .orElseThrow(()->new RuntimeException("bad link id"));
            return link.setName(path);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public <T> Stream<T> getDocumentLinks(RepositoryPath path, Query filter, boolean implicitRoot, Mapper<T> mapper) throws SQLException {
        LOG.entry(path, filter, mapper);
        Optional<RepositoryPath> basePath = getBasePath(path, mapper);
        if (!basePath.isPresent()) return Stream.empty();
        Stream<T> result = FluentStatement
            .of(searchDocumentLinkSQL(basePath.get(), path, filter, implicitRoot))
            .execute(schema.datasource, mapper);
        if (!implicitRoot && mapper == GET_LINK) {
            // Can't do this in simple map because of connection issues with deferred execution.
            // However, not a big deal as the primary use case for this is fetching the links related to a
            // particular document id; normally a small list.
            List<T> buffer = result.map(link->(T)getFullPath((DocumentLink)link)).collect(Collectors.toList());
            result.close();
            result = buffer.stream();
        }
        return result;
    }
    
    public void deleteObject(RepositoryPath path) throws SQLException, InvalidObjectName, InvalidWorkspace, InvalidWorkspaceState {
        LOG.entry(path);
        Workspace parent = getFolder(path.parent, GET_WORKSPACE).orElseThrow(()->new InvalidWorkspace(path.parent));
        if (parent.getState() != Workspace.State.Open) throw new InvalidWorkspaceState(path.parent, parent.getState());
        Id objectId = getInfo(path, GET_ID)
            .orElseThrow(()->new InvalidObjectName(path));
        FluentStatement.of(operations.deleteObject).set(1, objectId).execute(con);
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
