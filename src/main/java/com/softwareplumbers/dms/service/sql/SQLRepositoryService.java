/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.QualifiedName;
import com.softwareplumbers.common.abstractpattern.parsers.Parsers;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.dms.Constants;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.DocumentPart;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.NamedRepositoryObject;
import com.softwareplumbers.dms.Options;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryObject;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.StreamInfo;
import com.softwareplumbers.dms.service.sql.Filestore.NotFound;
import com.softwareplumbers.dms.service.sql.SQLAPI.Timestamped;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonObject;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

/** Implementation of Doctane RepositoryService using a SQL database and a Filestore.
 *
 * @author Jonathan Essex
 */
public class SQLRepositoryService implements RepositoryService {
    
    /** Options controlling DDL run on startup */
    public static enum CreateOption {
        /** Run the create and update scripts */ CREATE,
        /** Run just the update scripts */ UPDATE,
        /** Run the drop, create, and update scrips */ RECREATE
    }
    
    private static XLogger LOG = XLoggerFactory.getXLogger(SQLRepositoryService.class);
    
    private final SQLAPIFactory dbFactory;
    private Filestore<Id> filestore;
      
    private String getBaseName(JsonObject metadata) {
        return metadata.getString("DocumentTitle", "Document.dat");
    }
    
    private void maybeDestroyDocument(Id version) {
        try {
            filestore.remove(version);
        } catch (NotFound e) {
            LOG.catching(e);
        }
    }


    private Supplier<Exceptions.InvalidWorkspace> doThrowInvalidWorkspace(String id, QualifiedName path) {
        return ()->LOG.throwing(new Exceptions.InvalidWorkspace(id, path));
    }
       
    public SQLRepositoryService(SQLAPIFactory dbFactory, Filestore<Id> filestore, CreateOption option) throws SQLException {
        this.dbFactory = dbFactory;
        this.filestore = filestore;
        setCreateOption(option);
    }
    
    @Autowired
    public SQLRepositoryService(SQLAPIFactory dbFactory) {
        this.dbFactory = dbFactory;
        this.filestore = new LocalFilesystem();
    }
    
    @Required
    public void setFilestore(Filestore filestore) {
        this.filestore = filestore;
    }
    
    /** Control what operations will be performed on the database schema on startup. 
     * 
     * CREATE: will try to execute table creation scripts, and also recreate
     * any objects such as views and indexes which can be dropped and recreated
     * without destroying data.
     * 
     * UPDATE: will recreate any objects such as views and indexes which can be 
     * dropped and recreated without destroying data.
     * 
     * RECREATE: will delete all tables and objects and re-initialize the database.
     * 
     * @param option
     * @throws SQLException 
     */
    public final void setCreateOption(CreateOption option) throws SQLException {
        try (SQLAPI api = dbFactory.getSQLAPI()) {
            switch (option) {
                case CREATE: 
                    api.schema.createSchema();
                    api.schema.updateSchema();
                    break;
                case UPDATE:
                    api.schema.updateSchema();
                    break;
                case RECREATE: 
                    api.schema.dropSchema();
                    api.schema.createSchema();
                    api.schema.updateSchema();
                    break;
            }
        }
    }
    
    @Override
    public Reference createDocument(String mediaType, InputStreamSupplier iss, JsonObject metadata) {
        LOG.entry(mediaType, iss, metadata);
        Id version = filestore.generateKey();
        Id id = new Id();
        try (
            SQLAPI db = dbFactory.getSQLAPI();
        ) {
            StreamInfo info = StreamInfo.of(iss);
            filestore.put(version, info.supplier);
            db.createDocument(id, version, mediaType, info.length, info.digest, metadata);
            db.commit();
            return new Reference(id.toString(), version.toString());
        } catch (SQLException e) {
            maybeDestroyDocument(version);
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));           
        }
    }

    @Override
    public Reference updateDocument(String id, String mediaType, InputStreamSupplier iss, JsonObject metadata) throws Exceptions.InvalidDocumentId {
        LOG.entry(id, mediaType, iss, metadata);
        Id version = filestore.generateKey();
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Optional<Document> existing = db.getDocument(new Id(id), null, SQLAPI.GET_DOCUMENT);
            if (existing.isPresent()) {
                Document existingDoc = existing.get();
                mediaType = mediaType == Constants.NO_TYPE ? existingDoc.getMediaType() : mediaType;
                long length = existingDoc.getLength();
                byte[] digest = existingDoc.getDigest();
                Id replacing = new Id(existingDoc.getVersion());
                metadata = RepositoryObject.mergeMetadata(existingDoc.getMetadata(), metadata);
                if (iss != null) {
                    StreamInfo info = StreamInfo.of(iss);
                    length = info.length;
                    digest = info.digest;
                    filestore.put(version, info.supplier);
                } else {
                    filestore.link(replacing, version);
                }
                Id docId = new Id(id);
                db.createVersion(docId, version, mediaType, length, digest, metadata);
                db.commit();
            } else {
                throw LOG.throwing(new Exceptions.InvalidDocumentId(id));
            }
        } catch (SQLException e) {
            maybeDestroyDocument(version);
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));           
        } 
        return LOG.exit(new Reference(id, version.toString()));
    }
    

    @Override
    public DocumentLink getDocumentLink(String rootId, QualifiedName workspacePath, String documentId, Options.Get... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidDocumentId {
        LOG.entry(rootId, workspacePath, documentId, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id root = Id.of(rootId);
            QualifiedName rootPath = db.getPathTo(root).orElseThrow(()->new Exceptions.InvalidWorkspace(rootId));
            Optional<DocumentLink> current = db.getDocumentLink(root, workspacePath, Id.of(documentId), Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(rootPath));
            if (current.isPresent()) {
                return LOG.exit(current.get());
            } else {
                // TODO: check if workspace path is valid and return appropriate exception
                throw LOG.throwing(new Exceptions.InvalidDocumentId(documentId));
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } 
    }        

    @Override
    public DocumentLink createDocumentLink(String rootId, QualifiedName path, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        LOG.entry(rootId, path, mediaType, iss, metadata, Options.loggable(options));
        Id version = filestore.generateKey();
        Id id = new Id();
        Reference reference = new Reference(id.toString(), version.toString());
        if (metadata == null) metadata = JsonObject.EMPTY_JSON_OBJECT;

        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id idRoot = Id.of(rootId);
            Workspace folder = db.getOrCreateFolder(idRoot, path.parent, Options.CREATE_MISSING_PARENT.isIn(options), db::getWorkspace)
                .orElseThrow(doThrowInvalidWorkspace(rootId, path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            StreamInfo info = StreamInfo.of(iss);
            filestore.put(version, info.supplier);
            db.createDocument(id, version, mediaType, info.length, info.digest, metadata);
            DocumentLink result = db.createDocumentLink(Id.of(folder.getId()), path.part, id, version, SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            db.commit();
            return result;
        } catch (SQLException e) {
            maybeDestroyDocument(version);
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        }    
    }

    @Override
    public DocumentLink createDocumentLinkAndName(String rootId, QualifiedName workspaceName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState {
        
        Id version = filestore.generateKey();
        Id id = new Id();
        String baseName = getBaseName(metadata);

        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Workspace folder = db.getOrCreateFolder(Id.of(rootId), workspaceName, Options.CREATE_MISSING_PARENT.isIn(options), db::getWorkspace)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId, workspaceName));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            String name = db.generateUniqueName(folderId, baseName);
            StreamInfo info = StreamInfo.of(iss);
            db.createDocument(id, version, mediaType, info.length, info.digest, metadata);
            DocumentLink result = db.createDocumentLink(folderId, name, id, version, SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            db.commit();
            return result;        
        
        } catch (SQLException e) {
            maybeDestroyDocument(version);
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));            
        }
    }

    @Override
    public DocumentLink createDocumentLinkAndName(String rootId, QualifiedName workspaceName, Reference reference, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState, Exceptions.InvalidReference {
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Id root = Id.of(rootId);
            Workspace folder = db.getOrCreateFolder(Id.of(rootId), workspaceName, Options.CREATE_MISSING_PARENT.isIn(options), db::getWorkspace)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId, workspaceName));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            Id docId = Id.ofDocument(reference.id);
            Id versionId = filestore.parseKey(reference.version);
            Optional<DocumentLink> existing = db.getDocumentLink(folderId, QualifiedName.ROOT, docId, Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            if (existing.isPresent()) {
                if (Options.RETURN_EXISTING_LINK_TO_SAME_DOCUMENT.isIn(options)) {
                    return existing.get();
                } else {
                    throw LOG.throwing(new Exceptions.InvalidReference(reference));
                }
            }
            Document document = db.getDocument(docId, versionId, SQLAPI.GET_DOCUMENT).orElseThrow(()->new Exceptions.InvalidReference(reference));
            String name = db.generateUniqueName(folderId, getBaseName(document.getMetadata()));
            DocumentLink result = db.createDocumentLink(folderId, name, docId, versionId, SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            db.commit();               
            return result;
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public DocumentLink createDocumentLink(String rootId, QualifiedName path, Reference reference, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidReference, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Id root = Id.of(rootId);
            Workspace folder = db.getOrCreateFolder(Id.of(rootId), path.parent, Options.CREATE_MISSING_PARENT.isIn(options), db::getWorkspace)
                .orElseThrow(doThrowInvalidWorkspace(rootId, path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            Id docId = Id.ofDocument(reference.id);
            Id versionId = filestore.parseKey(reference.version);
            //Document document = db.getDocument(docId, versionId, SQLAPI.GET_DOCUMENT).orElseThrow(()->new Exceptions.InvalidReference(reference));
            DocumentLink result = db.createDocumentLink(folderId, path.part, docId, versionId,SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            db.commit();
            return result;
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    /** Update a document link
     * 
     * @param rootId
     * @param path - Cannot be empty
     * @param mediaType
     * @param iss
     * @param metadata
     * @param options
     * @return
     * @throws com.softwareplumbers.dms.Exceptions.InvalidWorkspace
     * @throws com.softwareplumbers.dms.Exceptions.InvalidObjectName
     * @throws com.softwareplumbers.dms.Exceptions.InvalidWorkspaceState 
     */
    @Override
    public DocumentLink updateDocumentLink(String rootId, QualifiedName path, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Update... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        
        Id version = filestore.generateKey();

        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Id root = Id.of(rootId);
            Workspace folder = db.getOrCreateFolder(Id.of(rootId), path.parent, Options.CREATE_MISSING_PARENT.isIn(options), db::getWorkspace)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId, path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());

            // We need to get the existing link in order to implement custom merge behavior
            Optional<DocumentLink> existing = db.getDocumentLink(folderId, QualifiedName.of(path.part), Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            if (existing.isPresent()) {
                DocumentLink existingDoc = existing.get();
                mediaType = mediaType == Constants.NO_TYPE ? existingDoc.getMediaType() : mediaType;
                long length = existingDoc.getLength();
                byte[] digest = existingDoc.getDigest();
                Reference replacing = existingDoc.getReference();
                metadata = RepositoryObject.mergeMetadata(existingDoc.getMetadata(), metadata);
                if (iss != null) {
                    StreamInfo info = StreamInfo.of(iss);
                    length = info.length;
                    digest = info.digest;
                    filestore.put(version, info.supplier);
                } else {
                    filestore.link(Id.ofDocument(replacing.version), version);
                }                
                
                Id replacingId = Id.ofDocument(replacing.id);
                db.createVersion(replacingId, version, mediaType, length, digest, metadata);
                DocumentLink result = db.updateDocumentLink(folderId, path.part, replacingId, version, SQLAPI.GET_LINK_WITH_PATH(folder.getName())).get();
                db.commit();           
                return result;
            } else {
                if (Options.CREATE_MISSING_ITEM.isIn(options)) {
                    Id docId = new Id();
                    StreamInfo info = StreamInfo.of(iss);
                    filestore.put(version, info.supplier);
                    db.createDocument(docId, version, mediaType, info.length, info.digest, metadata);
                    return db.createDocumentLink(folderId, path.part, docId, version, SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
                } else {
                    throw new Exceptions.InvalidObjectName(rootId, path);
                }
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));            
        }
    }

    @Override
    public DocumentLink updateDocumentLink(String rootId, QualifiedName path, Reference reference, Options.Update... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState, Exceptions.InvalidReference {
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Id root = Id.of(rootId);
            Id docId = Id.ofDocument(reference.id);
            Id versionId = filestore.parseKey(reference.version);
            Workspace folder = db.getOrCreateFolder(Id.of(rootId), path.parent, Options.CREATE_MISSING_PARENT.isIn(options), db::getWorkspace)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId, path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            Document document = db.getDocument(docId, versionId, SQLAPI.GET_DOCUMENT)
                .orElseThrow(()->new Exceptions.InvalidReference(reference));            
            Optional<DocumentLink> result = db.updateDocumentLink(folderId, path.part, docId, versionId, SQLAPI.GET_LINK_WITH_PATH(folder.getName()));
            if (!result.isPresent() && Options.CREATE_MISSING_ITEM.isIn(options)) {
                result = Optional.of(db.createDocumentLink(folderId, path.part, docId, versionId, SQLAPI.GET_LINK_WITH_PATH(folder.getName())));
            }
            db.commit();               
            return result.orElseThrow(()->LOG.throwing(new Exceptions.InvalidObjectName(rootId, path)));
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    
    
    @Override
    public NamedRepositoryObject copyObject(String rootId, QualifiedName path, String targetId, QualifiedName targetPath, boolean createParent) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(rootId, path, path, targetPath, createParent);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) { 
            Info info = db.getInfo(Id.of(rootId), path, SQLAPI.GET_INFO).orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path));
            switch (info.type) {
                case DOCUMENT_LINK: copyDocumentLink(rootId, path, targetId, targetPath, createParent);
                case WORKSPACE: copyWorkspace(rootId, path, targetId, targetPath, createParent);
                default:
                    throw LOG.throwing(new RuntimeException("Don't know how to copy " + info.type.toString()));
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }
    
    @Override
    public DocumentLink copyDocumentLink(String rootId, QualifiedName path, String targetId, QualifiedName targetPath, boolean createParent) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(rootId, path, targetId, targetPath, createParent);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) { 
            Id idTarget = Id.of(targetId);
            QualifiedName baseName = db.getPathTo(idTarget)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(targetId))
                .addAll(targetPath.parent);
            DocumentLink result = db.copyDocumentLink(Id.of(rootId), path, idTarget, targetPath, createParent, SQLAPI.GET_LINK_WITH_PATH(baseName));
            db.commit();
            return LOG.exit(result);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace copyWorkspace(String rootId, QualifiedName path, String targetId, QualifiedName targetPath, boolean createParent) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(rootId, path, targetId, targetPath, createParent);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) { 
            Id idTarget = Id.of(targetId);
            QualifiedName baseName = db.getPathTo(idTarget)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(targetId))
                .addAll(targetPath.parent);
            Workspace result = db.copyFolder(Id.of(rootId), path, idTarget, targetPath, createParent, rs->SQLAPI.getWorkspace(rs, baseName));
            db.commit();
            return LOG.exit(result);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new Exceptions.InvalidWorkspace(targetId, targetPath);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }  
    }

    @Override
    public Workspace createWorkspaceByName(String rootId, QualifiedName path, Workspace.State state, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace {
        LOG.entry(rootId, path, state, metadata, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            
            Id parent_id = path.parent.isEmpty() 
                ? Id.of(rootId)
                : db.getOrCreateFolder(Id.of(rootId), path.parent, Options.CREATE_MISSING_PARENT.isIn(options), SQLAPI.GET_ID).orElseThrow(()->new Exceptions.InvalidWorkspace(rootId, path.parent));
            Workspace result = db.createFolder(parent_id, path.part, state, metadata, rs->SQLAPI.getWorkspace(rs, path.parent));
            db.commit();
            return result;
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }    
    }

    @Override
    public Workspace createWorkspaceAndName(String rootId, QualifiedName workspacePath, Workspace.State state, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace {
        LOG.entry(rootId, workspacePath, state, metadata, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Id root = Id.of(rootId);
            Id folderId = workspacePath.isEmpty() 
                ? root 
                : db.getOrCreateFolder(root, workspacePath, Options.CREATE_MISSING_PARENT.isIn(options), SQLAPI.GET_ID)
                    .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId, workspacePath));
            String name = db.generateUniqueName(folderId, getBaseName(metadata));
            QualifiedName fullWorkspaceName = db.getPathTo(root)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId))
                .addAll(workspacePath);            
            Workspace result = db.createFolder(folderId, name, state, metadata, rs->SQLAPI.getWorkspace(rs, fullWorkspaceName));
            db.commit();               
            return LOG.exit(result);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace updateWorkspaceByName(String rootId, QualifiedName path, Workspace.State state, JsonObject metadata, Options.Update... options) throws Exceptions.InvalidWorkspace {
        LOG.entry(rootId, path, state, metadata, Options.loggable(options));
        boolean createMissing = Options.CREATE_MISSING_ITEM.isIn(options);

        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {    
            Id root = Id.of(rootId);
 
            Optional<Workspace> existing = db.getFolder(root, path, db::getWorkspace);
             
            if (existing.isPresent()) {
                Workspace existingWorkspace = existing.get();
                Id folderId = Id.of(existingWorkspace.getId());
                state = state == null ? existingWorkspace.getState() : state;
                if (!state.equals(existingWorkspace.getState())) {
                    switch (state) {
                        case Open: db.unlockVersions(folderId);
                        default: db.lockVersions(folderId);
                    }
                }
                metadata = RepositoryObject.mergeMetadata(existingWorkspace.getMetadata(), metadata);
                Workspace result = db.updateFolder(folderId, state, metadata, rs->SQLAPI.getWorkspace(rs, existingWorkspace.getName().parent))
                    .orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(rootId, path)));
                db.commit();           
                return LOG.exit(result);
            } else {
                if (createMissing && !path.isEmpty()) { // can't create a folder without a path
                    Id parentId = path.parent.isEmpty() 
                        ? root
                        : db.getOrCreateFolder(root, path.parent, Options.CREATE_MISSING_PARENT.isIn(options), SQLAPI.GET_ID)
                            .orElseThrow(doThrowInvalidWorkspace(rootId, path));
                    return LOG.exit(db.createFolder(parentId, path.part, state, metadata, rs->SQLAPI.getWorkspace(rs, path.parent)));
                } else {
                    throw LOG.throwing(new Exceptions.InvalidWorkspace(rootId, path));
                }
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } 
    }

    @Override
    public void deleteDocument(String rootId, QualifiedName workspacePath, String documentId) throws Exceptions.InvalidWorkspace, Exceptions.InvalidDocumentId, Exceptions.InvalidWorkspaceState {
        LOG.entry(rootId, workspacePath, documentId);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            db.deleteDocumentById(Id.of(rootId), workspacePath, Id.ofDocument(documentId));
            db.commit();
            LOG.exit();
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public void deleteObjectByName(String rootId, QualifiedName path) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        LOG.entry(rootId, path);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            db.deleteObject(Id.of(rootId), path);
            db.commit();
            LOG.exit();
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public <T> Optional<T> getImplementation(Class<T> type) {
        if (type.isAssignableFrom(this.getClass())) return Optional.of((T)this);
        return Optional.empty();
    }

    @Override
    public Document getDocument(Reference reference) throws Exceptions.InvalidReference {
        LOG.entry(reference);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            return LOG.exit(db.getDocument(Id.ofDocument(reference.id), filestore.parseKey(reference.version), SQLAPI.GET_DOCUMENT)
                .orElseThrow(()->new Exceptions.InvalidReference(reference))
            );
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public DocumentPart getPart(Reference rfrnc, QualifiedName qn) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream getData(Reference reference, Optional<QualifiedName> part) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName, IOException {
        LOG.entry(reference, part);
        return LOG.exit(filestore.get(filestore.parseKey(reference.version)));
    }

    @Override
    public void writeData(Reference reference, Optional<QualifiedName> part, OutputStream out) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName, IOException {
        LOG.entry(reference, part, out);
        OutputStreamConsumer.of(()->filestore.get(filestore.parseKey(reference.version))).consume(out);
        LOG.exit();
    }

    @Override
    public InputStream getData(String rootId, QualifiedName path, Options.Get... options) throws Exceptions.InvalidObjectName, IOException {
        LOG.entry(rootId, path, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            DocumentLink link = db.getDocumentLink(Id.of(rootId), path, Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(path.parent))
                .orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path));
            return LOG.exit(filestore.get(filestore.parseKey(link.getVersion())));
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public void writeData(String rootId, QualifiedName path, OutputStream out, Options.Get... options) throws Exceptions.InvalidObjectName, IOException {
        LOG.entry(rootId, path, out, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            DocumentLink link = db.getDocumentLink(Id.of(rootId), path, Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(path.parent))
                .orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path));
            OutputStreamConsumer.of(()->filestore.get(filestore.parseKey(link.getVersion()))).consume(out);
            LOG.exit();
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<DocumentPart> catalogueParts(Reference rfrnc, QualifiedName qn) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DocumentLink getDocumentLink(String rootId, QualifiedName path, Options.Get... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(rootId, path, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id id = Id.of(rootId);
            QualifiedName baseName = db.getPathTo(id)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId));
            DocumentLink link = db.getDocumentLink(id, path, Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(baseName))
                .orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path));
            return LOG.exit(link);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<Document> catalogue(Query query, boolean searchHistory) {
        LOG.entry(query, searchHistory);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {            
            Stream<Document> docs;
            if (searchHistory) {
                // Unfortunately this is supposed to return the most recent matching doc,
                // and since we don't plan to convert all metadata into database columns,
                // we can't do this until AFTER the filter operation is applied to the stream.
                final Comparator<Timestamped<Document>> SORT = (ta,tb)->ta.timestamp.compareTo(tb.timestamp);
                final Function<Timestamped<Document>,String> GROUP = t->t.value.getId();
                try (Stream<Timestamped<Document>> versions = db.getDocuments(query, true, SQLAPI.getTimestamped(SQLAPI.GET_DOCUMENT))) {
                    docs = versions
                        .filter(ts->query.containsItem(ts.value.toJson(this,0,1)))
                        .collect(Collectors.groupingBy(GROUP, Collectors.maxBy(SORT)))
                        .values()
                        .stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(ts->ts.value);
                }
            } else {
                docs = db.getDocuments(query, searchHistory, SQLAPI.GET_DOCUMENT)
                    .filter(link->query.containsItem(link.toJson(this,0,1)));
            }
            return LOG.exit(docs);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueById(String rootId, QualifiedName workspacePath, String documentId, Query query, Options.Search... options) throws Exceptions.InvalidWorkspace {
         LOG.entry(rootId, workspacePath, documentId, query, Options.loggable(options));
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id id = Id.of(rootId);
            QualifiedName baseName = db.getPathTo(id)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId));
            Stream<NamedRepositoryObject> links = db.getDocumentLinks(id, workspacePath, Id.of(documentId), query, SQLAPI.GET_LINK_WITH_PATH(baseName))
                .filter(link->query.containsItem(link.toJson(this,0,1)))
                .map(NamedRepositoryObject.class::cast);
            return LOG.exit(links);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueByName(String rootId, QualifiedName path, Query query, Options.Search... options) throws Exceptions.InvalidWorkspace {
        LOG.entry(rootId, path, Options.loggable(options));
        
        if (!Options.NO_IMPLICIT_WILDCARD.isIn(options)) {
            Predicate<String> hasWildcards = element -> !Parsers.parseUnixWildcard(element).isSimple();
            if (path.isEmpty() || path.indexFromEnd(hasWildcards) < 0) {
                path = path.add("*");
            }
        }
		
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id id = Id.of(rootId);
            QualifiedName baseName = db.getPathTo(id)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId));
            Stream<NamedRepositoryObject> links = db.getDocumentLinks(id, path, query, SQLAPI.GET_LINK_WITH_PATH(baseName))
                .filter(link->query.containsItem(link.toJson(this,1,0)))
                .map(NamedRepositoryObject.class::cast);
            Stream<NamedRepositoryObject> workspaces = db.getFolders(id, path, query, rs->SQLAPI.getWorkspace(rs, baseName))
                .filter(link->query.containsItem(link.toJson(this,1,0)))
                .map(NamedRepositoryObject.class::cast);
            return LOG.exit(Stream.concat(links, workspaces));
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<Document> catalogueHistory(Reference reference, Query query) throws Exceptions.InvalidReference {
        LOG.entry(reference, query);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {            
            Stream<Document> docs = db.getDocuments(Id.ofDocument(reference.id), query, SQLAPI.GET_DOCUMENT)
                .filter(link->query.containsItem(link.toJson(this,1,0)));
            return LOG.exit(docs);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public NamedRepositoryObject getObjectByName(String rootId, QualifiedName path, Options.Get... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(rootId, path);
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id id = Id.of(rootId);
            QualifiedName baseName = db.getPathTo(id)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId))
                .addAll(path)
                .parent;
            Info info = db.getInfo(id,path,SQLAPI.GET_INFO).orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path));
            switch(info.type) {
                case WORKSPACE:
                    return LOG.exit(db.getFolder(info.parent_id, QualifiedName.of(info.name), rs->SQLAPI.getWorkspace(rs, baseName))
                        .orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path)));
                case DOCUMENT_LINK:
                    return LOG.exit(db.getDocumentLink(info.parent_id, QualifiedName.of(info.name), Options.VERSION.getValue(options), SQLAPI.GET_LINK_WITH_PATH(baseName))
                        .orElseThrow(()->new Exceptions.InvalidObjectName(rootId, path)));
                default:
                    throw LOG.throwing(new RuntimeException("Don't know how to get " + info.type));
            }

        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace getWorkspaceByName(String rootId, QualifiedName path) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(rootId, path);
        if (path == null) path = QualifiedName.ROOT;
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id id = Id.of(rootId);
            QualifiedName baseName = db.getPathTo(id)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(rootId));
            Workspace workspace = db.getFolder(Id.of(rootId), path, rs->SQLAPI.getWorkspace(rs, baseName))
                .orElseThrow(doThrowInvalidWorkspace(rootId, path));
            return LOG.exit(workspace);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<DocumentLink> listWorkspaces(String documentId, QualifiedName pathFilter, Query filter, Options.Search... options) throws Exceptions.InvalidDocumentId {
        LOG.entry(pathFilter, documentId, filter, Options.loggable(options));
        if (pathFilter == null) pathFilter = QualifiedName.ROOT;
        try (
            SQLAPI db = dbFactory.getSQLAPI(); 
        ) {
            Id id = Id.ofDocument(documentId);
            Document document = db.getDocument(id, null, SQLAPI.GET_DOCUMENT)
                .orElseThrow(()->LOG.throwing(new Exceptions.InvalidDocumentId(documentId)));
            Stream<DocumentLink> links = db.getDocumentLinks(pathFilter, id, filter, db::getLink)
                .filter(link->filter.containsItem(link.toJson(this,1,0)));
            return LOG.exit(links);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }
    
}
