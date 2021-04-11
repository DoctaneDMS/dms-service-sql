/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.abstractpattern.Pattern;
import com.softwareplumbers.common.abstractpattern.parsers.Token;
import com.softwareplumbers.common.abstractpattern.parsers.Tokenizer;
import com.softwareplumbers.common.abstractpattern.visitor.Builders;
import com.softwareplumbers.common.abstractpattern.visitor.Visitor.PatternSyntaxException;
import com.softwareplumbers.common.abstractquery.Param;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.common.abstractquery.Range;
import com.softwareplumbers.common.immutablelist.QualifiedName;
import com.softwareplumbers.common.sql.AbstractInterface;
import com.softwareplumbers.common.sql.FluentStatement;
import com.softwareplumbers.common.sql.Mapper;
import com.softwareplumbers.dms.Constants;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryObject;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.VersionedRepositoryObject;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.DocumentImpl;
import com.softwareplumbers.dms.common.impl.DocumentLinkImpl;
import com.softwareplumbers.dms.common.impl.LocalData;
import com.softwareplumbers.dms.common.impl.WorkspaceImpl;
import com.softwareplumbers.dms.service.sql.DocumentDatabase.Operation;
import com.softwareplumbers.dms.service.sql.DocumentDatabase.Template;
import com.softwareplumbers.dms.service.sql.DocumentDatabase.EntityType;
import com.softwareplumbers.common.abstractquery.visitor.Visitors.ParameterizedSQL;
import com.softwareplumbers.dms.NamedRepositoryObject;
import com.softwareplumbers.dms.RepositoryPath.NamedElement;
import com.softwareplumbers.dms.RepositoryPath.Version;
import java.io.Reader;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;

/**
 *
 * @author jonathan
 */
public class DatabaseInterface extends AbstractInterface<DocumentDatabase.EntityType, DocumentDatabase.DataType, DocumentDatabase.Operation, DocumentDatabase.Template> {

    public DatabaseInterface(DocumentDatabase db) throws SQLException {
        super(db);
    }

    private static final String SAFE_CHARACTERS ="0123456789ABCDEFGHIJKLMNOPQURSTUVWYXabcdefghijklmnopqrstuvwxyz";
    private static final int MAX_SAFE_CHAR='z';
    private static final byte[] ROOT_ID = new byte[] { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };
    private static final String[] ESCAPE_IN_NAMES = new String[] {"@", "/", "~"};
    private static final int[] CP_ESCAPE_IN_NAMES = Stream.of(ESCAPE_IN_NAMES).mapToInt(s->s.codePointAt(0)).toArray();
    private static final Range NULL_VERSION = Range.equals(Json.createValue(""));
    private static final RepositoryPath ROOT_PATH = RepositoryPath.ROOT.addId(Id.ROOT_ID.toString());
    private static final Workspace ROOT_WORKSPACE = new WorkspaceImpl(Id.ROOT_ID.toString(), null, RepositoryPath.ROOT, false, Workspace.State.Open, Constants.EMPTY_METADATA, true, LocalData.NONE);
    
    private static boolean shouldDoubleEscape(int codepoint) {
        return Stream.of(ESCAPE_IN_NAMES).anyMatch(toEscape->toEscape.codePointAt(0) == codepoint);
    }
    
    public static String doubleEscape(String name) {
        StringBuilder builder = new StringBuilder();
        name.codePoints().flatMap(i -> shouldDoubleEscape(i) ? IntStream.of('\\', '\\', i) : IntStream.of(i)).forEachOrdered(builder::appendCodePoint);
        return builder.toString();
    }
    
    public static String unescapeName(String name) {

        StringBuilder builder = new StringBuilder();
        
        IntConsumer consumer = new IntConsumer() {
            boolean escaped = false;
            @Override public void accept(int i) {
                if (escaped) {
                    builder.appendCodePoint(i); escaped = false;
                } else {
                    if (i == '\\') escaped = true; else builder.appendCodePoint(i);
                }
            }
        };
                
        name.codePoints().forEachOrdered(consumer);
        return builder.toString();
    }
    
    public static String escapedUpdatePathElement(Pattern pattern) throws PatternSyntaxException {
        return pattern.build(Builders.toSimplePattern('\\', CP_ESCAPE_IN_NAMES));
    }
    
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
        Instant updateTime = results.getTimestamp("CREATED").toInstant();
        return new DocumentImpl(new Reference(id.toString(),version.toString()), updateTime, mediaType, length, hash, metadata, false, LocalData.NONE);
    };

    public static final Mapper<Reference> GET_REFERENCE = results -> {
        Id id = new Id(results.getBytes("DOCUMENT_ID"));
        Id version = new Id(results.getBytes("VERSION_ID"));
        return new Reference(id.toString(),version.toString());
    };

    public static RepositoryPath toRepositoryPath(String path) {
        Tokenizer tokenizer = new Tokenizer(path, '\\', ESCAPE_IN_NAMES);
        return RepositoryPathParser.parsePath(tokenizer);
    }
    
    public Optional<RepositoryPath> getBasePath(Id id) throws SQLException {
        try (Stream<RepositoryPath> names = operations.getStatement(Operation.fetchPathToId)
            .set(Types.ID, 1, id)
            .execute(con, rs->toRepositoryPath(rs.getString(1)))
        ) {

            return names.findFirst();
        }
    }

    public static DocumentLink getLink(ResultSet results, RepositoryPath basePath) throws SQLException {
        String mediaType = results.getString("MEDIA_TYPE");
        Id docId = new Id(results.getBytes("DOCUMENT_ID"));
        Id docVersion = new Id(results.getBytes("VERSION_ID"));
        long length = results.getLong("LENGTH");
        byte[] hash = results.getBytes("DIGEST");
        JsonObject metadata = toJson(results.getCharacterStream("METADATA"));
        Id id = new Id(results.getBytes("ID"));
        String version = results.getString("VERSION");
        Instant updateTime = results.getTimestamp("CREATED").toInstant();
        boolean deleted = results.getBoolean("DELETED");

        Optional<RepositoryPath.IdElement> rootId = basePath.getRootId();

        RepositoryPath name = basePath.addAll(toRepositoryPath(results.getString("PATH")));
        return new DocumentLinkImpl(id.toString(), version, name, deleted, new Reference(docId.toString(),docVersion.toString()), updateTime, mediaType, length, hash, metadata, false, LocalData.NONE);
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
        RepositoryPath path = toRepositoryPath(results.getString("PATH"));
        boolean deleted = results.getBoolean("DELETED");
        return new WorkspaceImpl(id.toString(), version, path, deleted, state, metadata, false, LocalData.NONE);
    };

    public static Mapper<Info> GET_INFO = results -> {
        Id id = new Id(results.getBytes("ID"));
        Id parent_id = Id.of(results.getBytes("PARENT_ID"));
        String name = results.getString("NAME");
        RepositoryObject.Type type = RepositoryObject.Type.valueOf(results.getString("TYPE"));
        boolean deleted = results.getBoolean("DELETED");
        return new Info(id, parent_id, name, type, deleted);
    };

    Query getVersionQuery(Version version) {
        if (version.getPattern().isPresent()) {
            if (version.getPattern().get().isSimple()) {
                return Query.from("version", Range.equals(Json.createValue(version.toString('\\', ESCAPE_IN_NAMES))));
            } else {
                return Query.from("version", Range.like(version.toString('\\', ESCAPE_IN_NAMES)));
            }
        }
        if (version.getId().isPresent()) {
            return Query.from("reference", Query.from("version", Range.equals(Json.createValue(version.getId().get())))); // was getName?
        }
        return Query.from("version", NULL_VERSION);
    }

    Query getParameterizedVersionQuery(Version version, String paramName) {
        if (version.getName().isPresent()) {
            return Query.from("version", Range.equals(Param.from(paramName)));
        }
        if (version.getId().isPresent()) {
            return Query.from("reference", Query.from("version", Range.equals(Param.from(paramName))));
        }
        return Query.from("version", Range.equals(Param.from(paramName)));
    }

    Query getNameQuery(RepositoryPath name) {

        if (name.isEmpty()) return Query.UNBOUNDED;

        Query result;

        if (name.parent.isEmpty()) {
            if (name.part.type != RepositoryPath.ElementType.ID) {
                result = Query.from("parentId", Range.equals(Json.createValue(Id.ROOT_ID.toString())));
            } else {
                result = Query.UNBOUNDED;
            }
        } else {
            // First get the query for the parent part of the name
            if (name.parent.part.type == RepositoryPath.ElementType.ID) {
                // this shortcut basically just avoids joining to the parent node if the criteria
                // is just on the node id
                result = Query.from("parentId", Range.like(((RepositoryPath.IdElement)name.parent.part).id));
            } else {
                result = Query.from("parent", getNameQuery(name.parent));
            }
        }

        // Now add the query for this part of the name
        switch (name.part.type) {
            case NAME:
                RepositoryPath.NamedElement pathElement = (RepositoryPath.NamedElement)name.part;
                String escapedName;
                try { escapedName = doubleEscape(pathElement.pattern.build(Builders.toUnixWildcard())); }
                catch (PatternSyntaxException e) {
                    throw new RuntimeException(e);
                }
                result = result.intersect(Query.from("name", Range.like(escapedName)));
                break;
            case ID:
                RepositoryPath.IdElement idElement = (RepositoryPath.IdElement)name.part;
                if (name.parent.isEmpty()) {
                    result = result.intersect(Query.from("id", Range.like(idElement.id)));
                } else {
                    result = result.intersect(Query.from("reference", Query.from("id", Range.equals(Json.createValue(idElement.id)))));
                }
                break;
            default:
                throw new RuntimeException("Unsupported element type in path " + name);
        }
        return result;
    }


    Query getVersionQuery(RepositoryPath name) {
        if (name.isEmpty()) return Query.UNBOUNDED;

        Query result = Query.from("parent", getVersionQuery(name.parent));

        // Now add the query for this part of the name
        switch (name.part.type) {
            case NAME:
                RepositoryPath.NamedElement pathElement = (RepositoryPath.NamedElement)name.part;
                result = result.intersect(getVersionQuery(pathElement.version));
                break;
            case ID:
                RepositoryPath.IdElement idElement = (RepositoryPath.IdElement)name.part;
                result = result.intersect(getVersionQuery(idElement.version));
                break;
            default:
                throw new RuntimeException("Unsupported element type in path " + name);
        }
        return result;
    }


    Query getDeletedQuery(RepositoryPath name) {

        if (name.isEmpty()) return Query.UNBOUNDED;

        Query result = Query.from("parent", getDeletedQuery(name.parent));

        if (name.part.getVersion() == Version.NONE)
            result = result.intersect(Query.from("deleted", Range.equals(JsonValue.FALSE)));

        return result;
    }


    Query getParameterizedNameQuery(String paramName, RepositoryPath name) {

        if (name.isEmpty()) return Query.from("id", Range.equals(Param.from(paramName)));

        Query result = Query.UNBOUNDED;

        // Now add the query for this part of the name
        switch (name.part.type) {
            case NAME:
                result = result.intersect(Query.from("name", Range.equals(Param.from(paramName))));
                result = result.intersect(getParameterizedVersionQuery(name.part.getVersion(), paramName+".version"));
                break;
            case ID:
                if (name.parent.isEmpty()) {
                    result = result.intersect(Query.from("id", Range.equals(Param.from(paramName))));
                } else {
                    result = result.intersect(Query.from("reference", Query.from("id", Range.equals(Param.from(paramName)))));
                    result = result.intersect(getParameterizedVersionQuery(name.part.getVersion(), paramName+".version"));
                }
                break;
            default:
                throw new RuntimeException("Unsupported element type in path " + name);
        }

        if (name.parent.isEmpty()) {
            if (name.part.type != RepositoryPath.ElementType.ID) {
                result = Query
                    .from("parentId", Range.equals(Param.from("parent." + paramName)))
                    .intersect(result);
            }
        } else {
            // First get the query for the parent part of the name
            if (name.parent.part.type == RepositoryPath.ElementType.ID) {
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
        int depth = path.afterRootId().size();
        String result = "'" + basePath.toString() + "'";
        for (int i = depth - 1; i >= 0 ; i--)
            result = templates.getSQL(Template.nameExpr, Integer.toString(i), result);
        return result;
    }

    ParameterizedSQL getParametrizedNameExpression(RepositoryPath path) {
        int depth = path.afterRootId().size();
        String result = "?";
        for (int i = depth - 1; i >= 0 ; i--)
            result = templates.getSQL(Template.nameExpr, Integer.toString(i), result);
        return new ParameterizedSQL(result, "basePath");
    }


    String getDocumentNameExpression(RepositoryPath basePath, RepositoryPath path) {
        int depth = path.afterRootId().size();
        String result = "'" + basePath.toString() + "'";
        for (int i = depth - 1; i > 0 ; i--)
            result = templates.getSQL(Template.nameExpr, Integer.toString(i), result);
        if (depth > 0) result = templates.getSQL(Template.documentNameExpr, "0", result);
        return result;
    }

    ParameterizedSQL getParametrizedDocumentNameExpression(RepositoryPath path) {
        int depth = path.afterRootId().size();
        String result = "?";
        for (int i = depth - 1; i > 0 ; i--)
            result = templates.getSQL(Template.nameExpr, Integer.toString(i), result);
        if (depth > 0) result = templates.getSQL(Template.documentNameExpr, "0", result);
        return new ParameterizedSQL(result, "basePath");
    }

    Query getDBFilterExpression(Iterable<QualifiedName> validFields, Query filter) {
        return StreamSupport.stream(validFields.spliterator(), false).reduce(Query.UNBOUNDED, (query, name) -> query.intersect(filter.getConstraint(name)), (query1, query2)->query1.intersect(query2));
    }

    ParameterizedSQL getInfoSQL(RepositoryPath path) {
        int depth = path.getDocumentPath().size();
        ParameterizedSQL criteria = path.size() > 1 && path.part.getId().isPresent()
            || !path.isEmpty() && path.part.getVersion().getId().isPresent()
            ? getParameterizedNameQuery("path", path).toExpression(schema.getFormatter(EntityType.LINK))
            : getParameterizedNameQuery("path", path).toExpression(schema.getFormatter(EntityType.NODE));
        return templates.getParameterizedSQL(Template.fetchInfo, criteria);
    }

    ParameterizedSQL getDocumentLinkSQL(RepositoryPath path) {
        Query query = getParameterizedNameQuery("path", path);
        if (path.part.getVersion() == Version.NONE)
            query = query.intersect(Query.from("current", Range.equals(JsonValue.TRUE)));
        //int depth = path.getDocumentPath().size();
        // This test is only needed
        //Type typeRequested = path.part.getVersion() != Version.NONE ? Type.DOCUMENT_LINK : Type.VERSION_LINK;
        ParameterizedSQL criteria = query.toExpression(schema.getFormatter(EntityType.LINK));
        ParameterizedSQL name =  getParametrizedDocumentNameExpression(path);
        return templates.getParameterizedSQL(Template.fetchDocumentLink, name, criteria);
    }

    String getDocumentSearchSQL(Query query, boolean searchHistory) {
        query = getDBFilterExpression(schema.getFields(EntityType.VERSION), query);
        if (!searchHistory)
            query = query.intersect(Query.from("latest", Range.equals(JsonValue.TRUE)));
        return templates.getSQL(Template.fetchDocument, query.toExpression(schema.getFormatter(EntityType.VERSION)).sql);
    }

    String getDocumentSearchHistorySQL(Query query) {
        query = getDBFilterExpression(schema.getFields(EntityType.VERSION), query);
        query = query.intersect(Query.from(QualifiedName.of("reference","id"), Range.equals(Param.from("0"))));
        return templates.getSQL(Template.fetchDocument, query.toExpression(schema.getFormatter(EntityType.VERSION)).sql);
    }

    String searchDocumentLinkSQL(RepositoryPath basePath, RepositoryPath nameWithPatterns, Query filter, boolean includeDeleted, boolean includeAllVersions) {
        filter = getDBFilterExpression(schema.getFields(EntityType.LINK), filter);
        // unless there is a wildcard in the version string for the final part of the address,
        // the intention of the user is probably to retrieve the current version of the document
        if (!includeAllVersions)
            filter = filter.intersect(Query.from("current", Range.equals(JsonValue.TRUE)));
        if (!nameWithPatterns.isEmpty()) {
            filter = filter
                .intersect(getNameQuery(nameWithPatterns))
                .intersect(getVersionQuery(nameWithPatterns));
        };
        if (!includeDeleted) {
            filter = filter
                .intersect(getDeletedQuery(nameWithPatterns));
        }
        return templates.getSQL(Template.fetchDocumentLink, getDocumentNameExpression(basePath, nameWithPatterns), filter.toExpression(schema.getFormatter(EntityType.LINK)).sql);
    }

    ParameterizedSQL getFolderSQL(RepositoryPath path) {
        int depth = path.getDocumentPath().size();
        ParameterizedSQL criteria = getParameterizedNameQuery("path", path).toExpression(schema.getFormatter(EntityType.FOLDER));
        ParameterizedSQL name = getParametrizedNameExpression(path);
        return templates.getParameterizedSQL(Template.fetchFolder, name, criteria);
    }

    String searchFolderSQL(RepositoryPath basePath, RepositoryPath nameWithPatterns, Query filter, boolean includeDeleted) {
        filter = getDBFilterExpression(schema.getFields(EntityType.FOLDER), filter);
        if (!nameWithPatterns.isEmpty()) {
            filter=filter
                .intersect(getNameQuery(nameWithPatterns))
                .intersect(getVersionQuery(nameWithPatterns));
        }
        if (!includeDeleted) {
            filter = filter
                .intersect(getDeletedQuery(nameWithPatterns));
        }
        return templates.getSQL(Template.fetchFolder, getNameExpression(basePath, nameWithPatterns), filter.toExpression(schema.getFormatter(EntityType.FOLDER)).sql);
    }

    public Optional<RepositoryPath> getPathTo(Id id) throws SQLException {
        LOG.entry(id);
        if (id.equals(Id.ROOT_ID))
            return LOG.exit(Optional.of(RepositoryPath.ROOT));
        else try (Stream<RepositoryPath> results = operations.getStatement(Operation.fetchPathToId)
                .set(Types.ID, 1, id)
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
        try (Stream<String> matches = operations.getStatement(Operation.fetchLastNameLike)
            .set(Types.ID, 1, id)
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
        operations.getStatement(Operation.createVersion)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, version)
            .set(3, mediaType)
            .set(4, length)
            .set(5, digest)
            .set(6, clobWriter(metadata))
            .execute(con);
        operations.getStatement(Operation.createDocument)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, version)
            .execute(con);
        LOG.exit();
    }

    public void createVersion(Id id, Id version, String mediaType, long length, byte[] digest, JsonObject metadata) throws SQLException {
        LOG.entry(mediaType, length, digest, metadata);
        operations.getStatement(Operation.createVersion)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, version)
            .set(3, mediaType)
            .set(4, length)
            .set(5, digest)
            .set(6, clobWriter(metadata))
            .execute(con);
        operations.getStatement(Operation.updateDocument)
            .set(Types.ID, 2, id)
            .set(Types.ID, 1, version)
            .execute(con);
        LOG.exit();
    }

    public <T> Optional<T> getDocument(Id id, Id version, Mapper<T> mapper) throws SQLException {
        LOG.entry(id, version , mapper);
        if (version == null) {
            try (Stream<T> result = operations.getStatement(Operation.fetchLatestDocument).set(Types.ID, 1, id).execute(con, mapper)) {
                return LOG.exit(result.findFirst());
            }
        } else {
            try (Stream<T> result = operations.getStatement(Operation.fetchDocument).set(Types.ID, 1, id).set(Types.ID, 2, version).execute(con, mapper)) {
                return LOG.exit(result.findFirst());
            }
        }
    }

    public <T> Stream<T> getDocuments(Id id, Query query, Mapper<T> mapper) throws SQLException {
        LOG.entry(id, query, mapper);
        Stream<T> result = FluentStatement.of(getDocumentSearchHistorySQL(query)).set(Types.ID, 1, id).execute(database.getDataSource(), mapper);
        return LOG.exit(result);
    }

    public <T> Stream<T> getDocuments(Query query, boolean searchHistory, Mapper<T> mapper) throws SQLException {
        LOG.entry(query, searchHistory, mapper);
        Stream<T> result = FluentStatement.of(getDocumentSearchSQL(query, searchHistory)).execute(database.getDataSource(), mapper);
        return LOG.exit(result);
    }

    public <T> Optional<RepositoryPath> getBasePath(RepositoryPath path, Mapper<T> mapper) throws SQLException {
        if (mapper == GET_WORKSPACE || mapper == GET_LINK) {
            Optional<RepositoryPath.IdElement> idElement = path.getRootId();
            if (idElement.isPresent()) {
                Id id = Id.of(idElement.get().id);
                return getPathTo(id);
            }
        }
        return Optional.of(RepositoryPath.ROOT);
    }

    public <T> T createFolder(Id parentId, Pattern name, Workspace.State state, JsonObject metadata, Mapper<T> mapper) throws SQLException, Exceptions.InvalidWorkspace {
        LOG.entry(parentId, name, state, metadata);
        Id id = new Id();
        operations.getStatement(Operation.createNode)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, parentId)
            .set(Types.NAME, 3, name)
            .set(4, RepositoryObject.Type.WORKSPACE.toString())
            .execute(con);
        operations.getStatement(Operation.createFolder)
            .set(Types.ID, 1, id)
            .set(2, state.toString())
            .set(3, clobWriter(metadata))
            .execute(con);

        RepositoryPath path = RepositoryPath.ROOT.addId(parentId.toString()).add(name);
        ParameterizedSQL sql = this.getFolderSQL(path);
        try (Stream<T> result = FluentStatement.of(sql.sql, sql.parameters)
            .set("basePath", getBasePath(path, mapper).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(path))).toString())
            .set(Types.PATH, "path", path)
            .execute(con, mapper)
        ) {
            return LOG.exit(
                result.findFirst()
                .orElseThrow(()->LOG.throwing(new RuntimeException("returned no results")))
            );
        }
    }

    public <T> Optional<T> getFolder(RepositoryPath name, Mapper<T> mapper) throws SQLException {
        LOG.entry(name);
        if (name.isEmpty() && (mapper == GET_ID || mapper == GET_VERIFIED_ID))
            return LOG.exit(Optional.of((T)Id.ROOT_ID));
        if (!name.isEmpty() && name.part.type == RepositoryPath.ElementType.ID && mapper == GET_ID) {
            // If all we need is the ID (because the mapper id GET_ID) and we have an Id on the path,
            // don't bother calling the database to verify.
            RepositoryPath.IdElement pathId = (RepositoryPath.IdElement)name.part;
            return LOG.exit(Optional.of((T)Id.of(pathId.id)));
        } else {
            Optional<RepositoryPath> basePath = getBasePath(name, mapper);
            if (!basePath.isPresent()) return LOG.exit(Optional.empty());
            ParameterizedSQL sql = getFolderSQL(name);
            try (Stream<T> result = FluentStatement.of(sql.sql, sql.parameters)
                .set("basePath",basePath.get().toString())
                .set(Types.PATH, "path", name)
                .execute(con, mapper)
            ) {
                return LOG.exit(result.findFirst());
            }
        }
    }

    public <T> Stream<T> getFolders(RepositoryPath path, Query filter, boolean includeDeleted, Mapper<T> mapper) throws Exceptions.InvalidWorkspace, SQLException {
        LOG.entry(path, mapper);
        if (path.isEmpty()) {
            // free search
            Stream<T> result = FluentStatement
                .of(searchFolderSQL(RepositoryPath.ROOT, path, filter, includeDeleted))
                .execute(database.getDataSource(), mapper);
            if (mapper == GET_WORKSPACE) {
                // Can't do this in simple map because of connection issues with deferred execution.
                // However, not a big deal as the primary use case for this is fetching the links related to a
                // particular document id; normally a small list.
                List<T> buffer = result.map(link->(T)getFullPath((Workspace)link)).collect(Collectors.toList());
                result.close();
                result = buffer.stream();
            }
            return LOG.exit(result);
        } else {
            Optional<RepositoryPath> basePath = getBasePath(path, mapper);
            // no base path implies we have an invalid root which does not exist
            if (!basePath.isPresent()) return LOG.exit(Stream.empty());
            Stream<T> result = FluentStatement
                .of(searchFolderSQL(basePath.get(), path, filter, includeDeleted))
                .execute(database.getDataSource(), mapper);
            return LOG.exit(result);
        }
    }

    @FunctionalInterface
    private interface ConsumerThrowing<T, E extends Exception>  {
        void consume(T item) throws E;
    }
    
    private <T,E extends Exception> void getInfos(RepositoryPath name, Mapper<T> mapper, ConsumerThrowing<T, E> consumer) throws SQLException, E {
        LOG.entry(name);
        Optional<RepositoryPath> basePath = getBasePath(name, mapper);
        if (!basePath.isPresent()) { LOG.exit(); return; }
        ParameterizedSQL sql = getInfoSQL(name);
        Iterator<T> items = FluentStatement.of(sql.sql, sql.parameters)
            .set(Types.PATH, "path", name)
            .execute(con, mapper)
            .iterator();
        while (items.hasNext()) consumer.consume(items.next());
        LOG.exit();
    }

    public <T> Optional<T> getInfo(RepositoryPath name, Mapper<T> mapper) throws SQLException {
        LOG.entry(name);
        Optional<RepositoryPath> basePath = getBasePath(name, mapper);
        if (!basePath.isPresent()) return LOG.exit(Optional.empty());
        ParameterizedSQL sql = getInfoSQL(name);
        try (Stream<T> results = FluentStatement.of(sql.sql, sql.parameters)
                .set(Types.PATH, "path", name)
                .execute(con, mapper)) 
        {
            return LOG.exit(results.findFirst());
        }
    }

    public Stream<Info> getChildren(Id parentId) throws SQLException {
        LOG.entry(parentId);
        return LOG.exit(operations.getStatement(Operation.fetchChildren)
            .set(Types.ID, 1, parentId)
            .execute(con, GET_INFO)
        );
    }

    public <T> Optional<T> getOrCreateFolder(Id parentId, Pattern name, boolean optCreate, Mapper<T> mapper) throws SQLException, Exceptions.InvalidWorkspace {
        RepositoryPath path = RepositoryPath.ROOT.addId(parentId.toString()).add(name);
        Optional<T> folder = getFolder(path, mapper);
        if (!folder.isPresent() && optCreate) {
            folder = Optional.of(createFolder(parentId, name, Workspace.State.Open, JsonObject.EMPTY_JSON_OBJECT, mapper));
        }
        return folder;
    }



    public <T> Optional<T> getOrCreateFolder(RepositoryPath path, boolean optCreate, Mapper<T> mapper) throws Exceptions.InvalidWorkspace, SQLException {
        LOG.entry(path, optCreate, mapper);

        if (path.isEmpty()) return LOG.exit(getFolder(path, mapper));

        switch (path.part.type) {
            case ID:
                return LOG.exit(getFolder(path, mapper)); // We can't create a folder without a name
            case NAME:
                RepositoryPath.NamedElement docPath = (RepositoryPath.NamedElement)path.part;
                if (path.parent.isEmpty()) {
                    return LOG.exit(getOrCreateFolder(Id.ROOT_ID, docPath.pattern, optCreate, mapper));
                } else {
                    Optional<Id> parentId = getOrCreateFolder(path.parent, optCreate, GET_ID);
                    if (parentId.isPresent()) {
                        return LOG.exit(getOrCreateFolder(parentId.get(), docPath.pattern, optCreate, mapper));
                    } else {
                        throw LOG.throwing(new Exceptions.InvalidWorkspace(path.parent));
                    }
                }
            default:
                throw LOG.throwing(new Exceptions.InvalidWorkspace(path));
        }

    }

    public <T> T copyFolder(RepositoryPath sourcePath, RepositoryPath targetPath, boolean optCreate, Mapper<T> mapper) throws SQLException, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspace {
        LOG.entry(sourcePath, targetPath, optCreate, mapper);
        Id idSrc = getFolder(sourcePath, GET_VERIFIED_ID)
            .orElseThrow(()->new Exceptions.InvalidWorkspace(sourcePath));
        Id folderId = getOrCreateFolder(targetPath.parent, optCreate, GET_ID)
            .orElseThrow(()->new Exceptions.InvalidWorkspace(targetPath.parent));

        if (targetPath.part.type != RepositoryPath.ElementType.NAME) throw LOG.throwing(new Exceptions.InvalidObjectName(targetPath));

        RepositoryPath.NamedElement docPart = (RepositoryPath.NamedElement)targetPath.part;

        Id id = new Id();
        operations.getStatement(Operation.createNode)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, folderId)
            .set(Types.NAME, 3, docPart.pattern)
            .set(4, RepositoryObject.Type.WORKSPACE.toString())
            .execute(con);
        operations.getStatement(Operation.copyFolder)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, idSrc)
            .execute(con);

        Iterable<Info> children = operations.getStatement(Operation.fetchChildren)
            .set(Types.ID, 1, idSrc)
            .execute(con, GET_INFO)
            .collect(Collectors.toList());
        for (Info child : children) {
            RepositoryPath srcPath = RepositoryPath.ROOT.addId(idSrc.toString()).add(child.name);
            RepositoryPath tgtPath = RepositoryPath.ROOT.addId(id.toString()).add(child.name);
            switch(child.type) {
                case WORKSPACE:
                    copyFolder(srcPath, tgtPath, false, GET_ID);
                    break;
                case DOCUMENT_LINK:
                    copyDocumentLink(srcPath, tgtPath, false, GET_ID);
                    break;
                default:
                    throw LOG.throwing(new RuntimeException("don't know how to copy " + child.type));
            }
        }
        RepositoryPath resultPath = RepositoryPath.ROOT.addId(folderId.toString()).add(docPart);
        ParameterizedSQL sql = getFolderSQL(resultPath);
        try (Stream<T> results = FluentStatement.of(sql.sql, sql.parameters)
            .set(1, getBasePath(resultPath, mapper).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(resultPath))).toString())
            .set(Types.PATH, "path", resultPath)
            .execute(con, mapper)
        ) {
            return LOG.exit(
                results.findFirst()
                .orElseThrow(()->LOG.throwing(new RuntimeException("returned no results"))));
        }
    }

    public <T> T copyDocumentLink(RepositoryPath sourcePath, RepositoryPath targetPath, boolean optCreate, Mapper<T> mapper) throws SQLException, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspace {
        LOG.entry(sourcePath, targetPath, optCreate, mapper);
        Id idSrc = getDocumentLink(sourcePath, GET_ID)
            .orElseThrow(()->LOG.throwing(new Exceptions.InvalidObjectName(sourcePath)));
        Id folderId = getOrCreateFolder(targetPath.parent, optCreate, GET_ID)
            .orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(targetPath.parent)));
        Id id = new Id();

        if (targetPath.part.type != RepositoryPath.ElementType.NAME) throw LOG.throwing(new Exceptions.InvalidObjectName(targetPath));
        RepositoryPath.NamedElement linkName = (RepositoryPath.NamedElement)targetPath.part;

        operations.getStatement(Operation.purgeChild)
            .set(Types.ID, 1, folderId)
            .set(Types.NAME, 2, linkName.pattern)
            .execute(con);
        operations.getStatement(Operation.createNode)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, folderId)
            .set(Types.NAME, 3, linkName.pattern)
            .set(4, RepositoryObject.Type.DOCUMENT_LINK.toString())
            .execute(con);
        operations.getStatement(Operation.copyLink)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, idSrc)
            .execute(con);

        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString()).add(linkName);

        ParameterizedSQL sql = getDocumentLinkSQL(shortResultPath);
        try (Stream<T> results = LOG.exit(FluentStatement.of(sql.sql, sql.parameters)
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(shortResultPath))).toString())
            .set(Types.PATH, "path", shortResultPath)
            .execute(con, mapper))) {

            return LOG.exit(results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results"))
            );
        }
    }

    public void copy(Id nodeId, Id newParentId) throws SQLException {
        LOG.entry(nodeId, newParentId);
        Id newId = new Id();
        operations.getStatement(Operation.copyNode)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, newParentId)
            .set(Types.ID, 3, nodeId)
            .execute(con);
        operations.getStatement(Operation.copyLink)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, nodeId)
            .execute(con);
        operations.getStatement(Operation.copyFolder)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, nodeId)
            .execute(con);
        Iterable<Id> children = operations.getStatement(Operation.fetchChildren)
            .set(Types.ID, 1, nodeId)
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
        operations.getStatement(Operation.copyNode)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, newParentId)
            .set(Types.ID, 3, nodeId)
            .execute(con);
        operations.getStatement(Operation.publishLink)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, nodeId)
            .execute(con);
        operations.getStatement(Operation.copyFolder)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, nodeId)
            .execute(con);
        Iterable<Id> children = operations.getStatement(Operation.fetchChildren)
            .set(Types.ID, 1, nodeId)
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
        operations.getStatement(Operation.publishNode)
            .set(Types.ID, 1, newId)
            .set(2, version)
            .set(Types.ID, 3, nodeId)
            .execute(con);
        int links = operations.getStatement(Operation.publishLink)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, nodeId)
            .execute(con);
        int folders = operations.getStatement(Operation.copyFolder)
            .set(Types.ID, 1, newId)
            .set(Types.ID, 2, nodeId)
            .execute(con);

        Iterable<Id> children = operations.getStatement(Operation.fetchChildren)
            .set(Types.ID, 1, nodeId)
            .execute(con, GET_ID)
            .collect(Collectors.toList());
        for (Id child : children) {
            publishChild(child, newId);
        }

        return LOG.exit(newId);
    }

    public <T> T createDocumentLink(Id folderId, Pattern name, Id docId, Id version, Mapper<T> mapper) throws SQLException, Exceptions.InvalidWorkspace {
        LOG.entry(folderId, name, docId, version);
        Id id = new Id();
        operations.getStatement(Operation.createNode)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, folderId)
            .set(Types.NAME, 3, name)
            .set(4, RepositoryObject.Type.DOCUMENT_LINK.toString())
            .execute(con);
        operations.getStatement(Operation.createLink)
            .set(Types.ID, 1, id)
            .set(Types.ID, 2, docId)
            .set(Types.ID, 3, version)
            .set(4, false)
            .execute(con);

        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString()).add(name);

        ParameterizedSQL sql = getDocumentLinkSQL(shortResultPath);
        try (Stream<T> results = FluentStatement.of(sql.sql, sql.parameters)
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(shortResultPath))).toString())
            .set(Types.PATH, "path", shortResultPath)
            .execute(con, mapper)) {
        return LOG.exit(
            results.findFirst()
                .orElseThrow(()->new RuntimeException("returned no results")));
        }
    }

    public <T> Optional<T> updateDocumentLink(Id folderId, Pattern name, Id docId, Id version, Mapper<T> mapper) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, SQLException {
        LOG.entry(folderId, name, docId, version);
        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString()).add(name);
        Optional<Info> info = getInfo(shortResultPath, GET_INFO);
        if (info.isPresent()) {
            operations.getStatement(Operation.updateLink)
                .set(Types.ID, 1, docId)
                .set(Types.ID, 2, version)
                .set(Types.ID, 3, info.get().id)
                .execute(con);

            ParameterizedSQL sql = getDocumentLinkSQL(shortResultPath);
            try (Stream<T> results = FluentStatement.of(sql.sql, sql.parameters)
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(shortResultPath))).toString())
            .set(Types.PATH, "path", shortResultPath)
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

    public <T> Optional<T> updateFolder(Id folderId, Workspace.State state, JsonObject metadata, Mapper<T> mapper) throws Exceptions.InvalidWorkspace, SQLException {
        LOG.entry(folderId, state, metadata);
        RepositoryPath shortResultPath = RepositoryPath.ROOT.addId(folderId.toString());
        int count = operations.getStatement(Operation.updateFolder)
            .set(1, state.toString())
            .set(2, metadata)
            .set(Types.ID, 3, folderId)
            .execute(con);
        if (count == 0) return Optional.empty();
        ParameterizedSQL sql = getFolderSQL(shortResultPath);
        try (Stream<T> result = FluentStatement.of(sql.sql, sql.parameters)
            .set(1, getBasePath(shortResultPath, mapper).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(shortResultPath))).toString())
            .set(Types.PATH, "path", shortResultPath)
            .execute(con, mapper)
        ) {
            return LOG.exit(
                result.findFirst()
            );
        }
    }

    public void lockVersions(Id folderId) throws SQLException {
        LOG.entry(folderId);
        operations.getStatement(Operation.lockVersions)
            .set(Types.ID, 1, folderId)
            .execute(con);
        LOG.exit();
    }

    public void unlockVersions(Id folderId) throws SQLException {
        LOG.entry(folderId);
        operations.getStatement(Operation.unlockVersions)
            .set(Types.ID, 1, folderId)
            .execute(con);
        LOG.exit();
    }

    public <T> Optional<T> getDocumentLink(RepositoryPath path, Mapper<T> mapper) throws SQLException {
        LOG.entry(path, mapper);
        Optional<RepositoryPath> basePath = getBasePath(path, mapper);
        if (!basePath.isPresent()) return Optional.empty();
        ParameterizedSQL sql = getDocumentLinkSQL(path);
        try (Stream<T> result = FluentStatement
            .of(sql.sql, sql.parameters)
            .set(1, basePath.get().toString())
            .set(Types.PATH, "path", path)
            .execute(con, mapper)
        ) {
            return LOG.exit(result.findFirst());
        }
    }

    public <T extends VersionedRepositoryObject> T getFullPath(T link) {
        try {
            RepositoryPath path = getPathTo(Id.of(link.getId()))
                .orElseThrow(()->LOG.throwing(new RuntimeException("bad link id")));
            return (T)link.setName(path);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> Stream<T> getDocumentLinks(RepositoryPath path, Query filter, boolean includeDeleted, boolean includeAllVersions, Mapper<T> mapper) throws SQLException {
        LOG.entry(path, filter, mapper);

        if (path.isEmpty()) {
            Stream<T> result = FluentStatement
                .of(searchDocumentLinkSQL(RepositoryPath.ROOT, path, filter, includeDeleted, includeAllVersions))
                .execute(database.getDataSource(), mapper);
            if (mapper == GET_LINK) {
                // Can't do this in simple map because of connection issues with deferred execution.
                // However, not a big deal as the primary use case for this is fetching the links related to a
                // particular document id; normally a small list.
                List<T> buffer = result.map(link->(T)getFullPath((DocumentLink)link)).collect(Collectors.toList());
                result.close();
                result = buffer.stream();
            }
            return LOG.exit(result);
        } else {
            Optional<RepositoryPath> basePath = getBasePath(path, mapper);
            if (!basePath.isPresent()) return LOG.exit(Stream.empty());
            Stream<T> result = FluentStatement
                .of(searchDocumentLinkSQL(basePath.get(), path, filter, includeDeleted, includeAllVersions))
                .execute(database.getDataSource(), mapper);
            return result;
        }
    }

    public Stream<NamedRepositoryObject> deleteObject(RepositoryPath path) throws SQLException, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState {
        LOG.entry(path);
        Workspace parent = getFolder(path.parent, GET_WORKSPACE).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(path.parent)));
        if (parent.getState() != Workspace.State.Open) throw LOG.throwing(new Exceptions.InvalidWorkspaceState(path.parent, parent.getState()));
        List<NamedRepositoryObject> result = new ArrayList<>();
        getInfos(path, GET_INFO, info -> { 
            operations.getStatement(Operation.deleteObject).set(Types.ID, 1, info.id).execute(con);
            switch(info.type) {
                case DOCUMENT_LINK:
                    result.add(getDocumentLink(path, GET_LINK).orElseThrow(()->new RuntimeException("failed to fetch deleted link")));
                    break;
                case WORKSPACE:
                    result.add(getFolder(path, GET_WORKSPACE).orElseThrow(()->new RuntimeException("failed to fetch deleted workspace")));
                    break;
                default:
                    throw new RuntimeException("unknown type " + info.type);
              }
        });
        return LOG.exit(result.stream());
    }

    public Stream<NamedRepositoryObject> undeleteObject(RepositoryPath path) throws SQLException, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState {
        LOG.entry(path);
        Workspace parent = getFolder(path.parent, GET_WORKSPACE).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(path.parent)));
        if (parent.getState() != Workspace.State.Open) throw LOG.throwing(new Exceptions.InvalidWorkspaceState(path.parent, parent.getState()));
        List<NamedRepositoryObject> result = new ArrayList<>();
        getInfos(path, GET_INFO, info -> { 
            operations.getStatement(Operation.undeleteObject).set(Types.ID, 1, info.id).execute(con); 
            switch(info.type) {
                case DOCUMENT_LINK:
                    result.add(getDocumentLink(path, GET_LINK).orElseThrow(()->new RuntimeException("failed to fetch undeleted link")));
                    break;
                case WORKSPACE:
                    result.add(getFolder(path, GET_WORKSPACE).orElseThrow(()->new RuntimeException("failed to fetch undeleted workspace")));
                    break;
                default:
                    throw new RuntimeException("unknown type " + info.type);
            }
        });
        return LOG.exit(result.stream());
    }

    void updateDigest(Reference reference, byte[] digest) throws SQLException {
        LOG.entry(reference, digest);
        operations.getStatement(Operation.updateDigest)
            .set(1, digest)
            .set(Types.ID, 2, Id.of(reference.id))
            .set(Types.ID, 3, Id.of(reference.version))
            .execute(con);
        LOG.exit();
    }
}
