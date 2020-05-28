/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

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
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.RepositoryPath.ElementType;
import com.softwareplumbers.dms.RepositoryPath.NamedElement;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.DocumentImpl;
import com.softwareplumbers.dms.common.impl.LocalData;
import com.softwareplumbers.dms.common.impl.StreamInfo;
import com.softwareplumbers.dms.service.sql.Filestore.NotFound;
import com.softwareplumbers.dms.service.sql.DatabaseInterface.Timestamped;
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

    private static XLogger LOG = XLoggerFactory.getXLogger(SQLRepositoryService.class);
    
    private final DocumentDatabase dbFactory;
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


    private Supplier<Exceptions.InvalidWorkspace> doThrowInvalidWorkspace(RepositoryPath path) {
        return ()->LOG.throwing(new Exceptions.InvalidWorkspace(path));
    }
       
    public SQLRepositoryService(DocumentDatabase dbFactory, Filestore<Id> filestore) throws SQLException {
        this.dbFactory = dbFactory;
        this.filestore = filestore;
    }
    
    @Autowired
    public SQLRepositoryService(DocumentDatabase dbFactory) {
        this.dbFactory = dbFactory;
        this.filestore = new LocalFilesystem();
    }
    
    @Required
    public void setFilestore(Filestore filestore) {
        this.filestore = filestore;
    }
    

    @Override
    public Reference createDocument(String mediaType, InputStreamSupplier iss, JsonObject metadata) {
        LOG.entry(mediaType, iss, metadata);
        Id version = filestore.generateKey();
        Id id = new Id();
        try (
            DatabaseInterface db = dbFactory.getInterface();
        ) {
            StreamInfo info = StreamInfo.of(iss);
            Document document = new DocumentImpl(new Reference(id.toString(), version.toString()), mediaType, info.length, info.digest, metadata, false, LocalData.NONE);
            filestore.put(version, document, info);
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
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Optional<Document> existing = db.getDocument(new Id(id), null, DatabaseInterface.GET_DOCUMENT);
            if (existing.isPresent()) {
                Document existingDoc = existing.get();
                mediaType = mediaType == Constants.NO_TYPE ? existingDoc.getMediaType() : mediaType;
                long length = existingDoc.getLength();
                byte[] digest = existingDoc.getDigest();
                Id replacing = new Id(existingDoc.getReference().version);
                metadata = RepositoryObject.mergeMetadata(existingDoc.getMetadata(), metadata);
                if (iss != null) {
                    StreamInfo info = StreamInfo.of(iss);
                    length = info.length;
                    digest = info.digest;
                    filestore.put(version, existingDoc.setMetadata(metadata), info);
                } else {
                    filestore.link(existingDoc.setMetadata(metadata), replacing, version);
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
    public DocumentLink getDocumentLink(RepositoryPath workspacePath, Options.Get... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(workspacePath, Options.loggable(options));
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Optional<DocumentLink> current = db.getDocumentLink(workspacePath, DatabaseInterface.GET_LINK);
            if (current.isPresent()) {
                return LOG.exit(current.get());
            } else {
                // TODO: check if workspace path is valid and return appropriate exception
                throw LOG.throwing(new Exceptions.InvalidObjectName(workspacePath));
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } 
    }        

    @Override
    public DocumentLink createDocumentLink(RepositoryPath path, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        LOG.entry(path, mediaType, iss, metadata, Options.loggable(options));
        Id version = filestore.generateKey();
        Id id = new Id();
        Reference reference = new Reference(id.toString(), version.toString());
        if (metadata == null) metadata = JsonObject.EMPTY_JSON_OBJECT;

        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Workspace folder = db.getOrCreateFolder(path.parent, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(doThrowInvalidWorkspace(path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            if (path.part.type != ElementType.NAME)
                throw LOG.throwing(new Exceptions.InvalidObjectName(path));
            NamedElement linkPart = (NamedElement)path.part;
            StreamInfo info = StreamInfo.of(iss);
            
            
            Document document = new DocumentImpl(new Reference(id.toString(), version.toString()), mediaType, info.length, info.digest, metadata, false, LocalData.NONE);
            filestore.put(version, document, info);
            db.createDocument(id, version, mediaType, info.length, info.digest, metadata);
            
            DocumentLink result = db.createDocumentLink(Id.of(folder.getId()), linkPart.name, Id.ofDocument(document.getReference().id), Id.ofVersion(document.getReference().version), DatabaseInterface.GET_LINK);
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
    public DocumentLink createDocumentLinkAndName(RepositoryPath workspaceName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState {
        LOG.entry(workspaceName, mediaType, iss, metadata, Options.loggable(options));
        Id version = filestore.generateKey();
        Id id = new Id();
        String baseName = getBaseName(metadata);

        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {    
            Workspace folder = db.getOrCreateFolder(workspaceName, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(workspaceName));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            String name = db.generateUniqueName(folderId, baseName);
            StreamInfo info = StreamInfo.of(iss);
            Document document = new DocumentImpl(new Reference(id.toString(), version.toString()), mediaType, info.length, info.digest, metadata, false, LocalData.NONE);
            filestore.put(version, document, info);
            db.createDocument(id, version, mediaType, info.length, info.digest, metadata);
            DocumentLink result = db.createDocumentLink(folderId, name, id, version, DatabaseInterface.GET_LINK);
            db.commit();
            return LOG.exit(result);        
        
        } catch (SQLException e) {
            maybeDestroyDocument(version);
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));            
        }
    }

    @Override
    public DocumentLink createDocumentLinkAndName(RepositoryPath workspaceName, Reference reference, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState, Exceptions.InvalidReference {
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {    
            Workspace folder = db.getOrCreateFolder(workspaceName, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(workspaceName));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            RepositoryPath existingName = workspaceName.addId(reference.getId());
            Optional<DocumentLink> existing = db.getDocumentLink(existingName, DatabaseInterface.GET_LINK);
            if (existing.isPresent()) {
                if (Options.RETURN_EXISTING_LINK_TO_SAME_DOCUMENT.isIn(options)) {
                    return existing.get();
                } else {
                    throw LOG.throwing(new Exceptions.InvalidReference(reference));
                }
            }
            Id docId = Id.ofDocument(reference.id);
            Id versionId = Id.ofVersion(reference.version);
            Id folderId = Id.of(folder.getId());
            Document document = db.getDocument(docId, versionId, DatabaseInterface.GET_DOCUMENT).orElseThrow(()->new Exceptions.InvalidReference(reference));
            String name = db.generateUniqueName(folderId, getBaseName(document.getMetadata()));
            DocumentLink result = db.createDocumentLink(folderId, name, docId, versionId, DatabaseInterface.GET_LINK);
            db.commit();               
            return result;
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public DocumentLink createDocumentLink(RepositoryPath path, Reference reference, Options.Create... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidReference, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {    
            Workspace folder = db.getOrCreateFolder(path.parent, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(doThrowInvalidWorkspace(path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            Id docId = Id.ofDocument(reference.id);
            Id versionId = filestore.parseKey(reference.version);
            //Document document = db.getDocument(docId, versionId, DatabaseInterface.GET_DOCUMENT).orElseThrow(()->new Exceptions.InvalidReference(reference));
            NamedElement namePart = (NamedElement)path.part;
            DocumentLink result = db.createDocumentLink(folderId, namePart.name, docId, versionId, DatabaseInterface.GET_LINK);
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
    public DocumentLink updateDocumentLink(RepositoryPath path, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Update... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        
        Id version = filestore.generateKey();

        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {    
            Workspace folder = db.getOrCreateFolder(path.parent, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());

            // We need to get the existing link in order to implement custom merge behavior
            RepositoryPath shortPath = RepositoryPath.ROOT.addId(folder.getId()).add(path.part);
            NamedElement part = (NamedElement)path.part;
            Optional<DocumentLink> existing = db.getDocumentLink(shortPath, DatabaseInterface.GET_LINK);
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
                    filestore.put(version, existingDoc.setMetadata(metadata), info);
                } else {
                    filestore.link(existingDoc.setMetadata(metadata), Id.ofDocument(replacing.version), version);
                }                
                
                Id replacingId = Id.ofDocument(replacing.id);
                db.createVersion(replacingId, version, mediaType, length, digest, metadata);
                DocumentLink result = db.updateDocumentLink(folderId, part.name, replacingId, version, DatabaseInterface.GET_LINK).get();
                db.commit();           
                return result;
            } else {
                if (Options.CREATE_MISSING_ITEM.isIn(options)) {
                    Id docId = new Id();
                    StreamInfo info = StreamInfo.of(iss);
                    Document document = new DocumentImpl(new Reference(docId.toString(), version.toString()), mediaType, info.length, info.digest, metadata, false, LocalData.NONE);
                    filestore.put(version, document, info);
                    db.createDocument(docId, version, mediaType, info.length, info.digest, metadata);
                    DocumentLink result = db.createDocumentLink(folderId, part.name, docId, version, DatabaseInterface.GET_LINK);
                    db.commit();
                    return result;
                } else {
                    throw new Exceptions.InvalidObjectName(path);
                }
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));            
        }
    }

    @Override
    public DocumentLink updateDocumentLink(RepositoryPath path, Reference reference, Options.Update... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState, Exceptions.InvalidReference {
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {    
            Id docId = Id.ofDocument(reference.id);
            Id versionId = filestore.parseKey(reference.version);
            NamedElement part = (NamedElement)path.part;
            Workspace folder = db.getOrCreateFolder(path.parent, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(path.parent));
            if (folder.getState() != Workspace.State.Open)
                throw LOG.throwing(new Exceptions.InvalidWorkspaceState(folder.getName(), folder.getState()));
            Id folderId = Id.of(folder.getId());
            Document document = db.getDocument(docId, versionId, DatabaseInterface.GET_DOCUMENT)
                .orElseThrow(()->new Exceptions.InvalidReference(reference));            
            Optional<DocumentLink> result = db.updateDocumentLink(folderId, part.name, docId, versionId, DatabaseInterface.GET_LINK);
            if (!result.isPresent() && Options.CREATE_MISSING_ITEM.isIn(options)) {
                result = Optional.of(db.createDocumentLink(folderId, part.name, docId, versionId, DatabaseInterface.GET_LINK));
            }
            db.commit();               
            return result.orElseThrow(()->LOG.throwing(new Exceptions.InvalidObjectName(path)));
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    
    
    @Override
    public NamedRepositoryObject copyObject(RepositoryPath path, RepositoryPath targetPath, boolean createParent) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(path, path, targetPath, createParent);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) { 
            Info info = db.getInfo(path, DatabaseInterface.GET_INFO).orElseThrow(()->new Exceptions.InvalidObjectName(path));
            switch (info.type) {
                case DOCUMENT_LINK: copyDocumentLink(path, targetPath, createParent);
                case WORKSPACE: copyWorkspace(path, targetPath, createParent);
                default:
                    throw LOG.throwing(new RuntimeException("Don't know how to copy " + info.type.toString()));
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }
    
    @Override
    public DocumentLink copyDocumentLink(RepositoryPath path, RepositoryPath targetPath, boolean createParent) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(path, targetPath, createParent);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) { 
            DocumentLink result = db.copyDocumentLink(path, targetPath, createParent, DatabaseInterface.GET_LINK);
            db.commit();
            return LOG.exit(result);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace copyWorkspace(RepositoryPath path, RepositoryPath targetPath, boolean createParent) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(path, targetPath, createParent);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) { 
            Workspace result = db.copyFolder(path, targetPath, createParent, DatabaseInterface.GET_WORKSPACE);
            db.commit();
            return LOG.exit(result);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new Exceptions.InvalidWorkspace(targetPath);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }  
    }

    @Override
    public Workspace createWorkspaceByName(RepositoryPath path, Workspace.State state, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace {
        LOG.entry(path, state, metadata, Options.loggable(options));
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            
            Id parent_id = db.getOrCreateFolder(path.parent, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_ID).orElseThrow(()->new Exceptions.InvalidWorkspace(path.parent));
            NamedElement part = (NamedElement)path.part;
            Workspace result = db.createFolder(parent_id, part.name, state, metadata, DatabaseInterface.GET_WORKSPACE);
            db.commit();
            return result;
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }    
    }

    @Override
    public Workspace createWorkspaceAndName(RepositoryPath workspacePath, Workspace.State state, JsonObject metadata, Options.Create... options) throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace {
        LOG.entry(workspacePath, state, metadata, Options.loggable(options));
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {    
            Id folderId = db.getOrCreateFolder(workspacePath, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_ID)
                .orElseThrow(()->new Exceptions.InvalidWorkspace(workspacePath));
            String name = db.generateUniqueName(folderId, getBaseName(metadata));
            Workspace result = db.createFolder(folderId, name, state, metadata, DatabaseInterface.GET_WORKSPACE);
            db.commit();               
            return LOG.exit(result);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace updateWorkspaceByName(RepositoryPath path, Workspace.State state, JsonObject metadata, Options.Update... options) throws Exceptions.InvalidWorkspace {
        LOG.entry(path, state, metadata, Options.loggable(options));
        boolean createMissing = Options.CREATE_MISSING_ITEM.isIn(options);

        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {     
            Optional<Workspace> existing = db.getFolder(path, DatabaseInterface.GET_WORKSPACE);
             
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
                Workspace result = db.updateFolder(folderId, state, metadata, DatabaseInterface.GET_WORKSPACE)
                    .orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(path)));
                db.commit();           
                return LOG.exit(result);
            } else {
                if (createMissing && !path.isEmpty()) { // can't create a folder without a path
                    Id parentId = db.getOrCreateFolder(path.parent, Options.CREATE_MISSING_PARENT.isIn(options), DatabaseInterface.GET_ID)
                            .orElseThrow(doThrowInvalidWorkspace(path));
                    NamedElement part = (NamedElement)path.part;
                    Workspace result = db.createFolder(parentId, part.name, state, metadata, DatabaseInterface.GET_WORKSPACE);
                    db.commit();
                    return LOG.exit(result);
                } else {
                    throw LOG.throwing(new Exceptions.InvalidWorkspace(path));
                }
            }
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } 
    }

    @Override
    public void deleteDocument(RepositoryPath workspacePath, String documentId) throws Exceptions.InvalidWorkspace, Exceptions.InvalidDocumentId, Exceptions.InvalidWorkspaceState {
        LOG.entry(workspacePath, documentId);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            db.deleteObject(workspacePath.addId(documentId));
            db.commit();
            LOG.exit();
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (Exceptions.InvalidObjectName e) {
            throw LOG.throwing(new Exceptions.InvalidDocumentId(documentId));
        }
    }

    @Override
    public void deleteObjectByName(RepositoryPath path) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidWorkspaceState {
        LOG.entry(path);
        if (path.isEmpty()) throw new Exceptions.InvalidObjectName(path);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            db.deleteObject(path);
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
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            return LOG.exit(db.getDocument(Id.ofDocument(reference.id), filestore.parseKey(reference.version), DatabaseInterface.GET_DOCUMENT)
                .orElseThrow(()->new Exceptions.InvalidReference(reference))
            );
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public DocumentPart getPart(Reference rfrnc, RepositoryPath qn) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream getData(RepositoryPath path, Options.Get... options) throws Exceptions.InvalidObjectName, IOException {
        LOG.entry(path, Options.loggable(options));
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            DocumentLink link = db.getDocumentLink(path, DatabaseInterface.GET_LINK)
                .orElseThrow(()->LOG.throwing(new Exceptions.InvalidObjectName(path)));
            return LOG.exit(filestore.get(filestore.parseKey(link.getReference().version)));
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public void writeData(RepositoryPath path, OutputStream out, Options.Get... options) throws Exceptions.InvalidObjectName, IOException {
        LOG.entry(path, out, Options.loggable(options));
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            DocumentLink link = db.getDocumentLink(path, DatabaseInterface.GET_LINK)
                .orElseThrow(()->new Exceptions.InvalidObjectName(path));
            OutputStreamConsumer.of(()->filestore.get(filestore.parseKey(link.getReference().version))).consume(out);
            LOG.exit();
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<DocumentPart> catalogueParts(Reference rfrnc, RepositoryPath qn) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<Document> catalogue(Query query, boolean searchHistory) {
        LOG.entry(query, searchHistory);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {            
            Stream<Document> docs;
            if (searchHistory) {
                // Unfortunately this is supposed to return the most recent matching doc,
                // and since we don't plan to convert all metadata into database columns,
                // we can't do this until AFTER the filter operation is applied to the stream.
                final Comparator<Timestamped<Document>> SORT = (ta,tb)->ta.timestamp.compareTo(tb.timestamp);
                final Function<Timestamped<Document>,String> GROUP = t->t.value.getReference().id;
                try (Stream<Timestamped<Document>> versions = db.getDocuments(query, true, DatabaseInterface.getTimestamped(DatabaseInterface.GET_DOCUMENT))) {
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
                docs = db.getDocuments(query, searchHistory, DatabaseInterface.GET_DOCUMENT)
                    .filter(link->query.containsItem(link.toJson(this,0,1)));
            }
            return LOG.exit(docs);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueByName(RepositoryPath path, Query query, Options.Search... options) throws Exceptions.InvalidWorkspace {
        LOG.entry(path, Options.loggable(options));
        boolean hasDocumentId = path.size() > 1 && path.part.getId().isPresent();
        if (!Options.NO_IMPLICIT_WILDCARD.isIn(options) && !path.isEmpty() && !hasDocumentId) {
            if (path.isEmpty() || !path.find(RepositoryPath::isWildcard).isPresent()) {
                path = path.add("*");
            }
        }
		
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Stream<NamedRepositoryObject> links = db.getDocumentLinks(path, query, DatabaseInterface.GET_LINK)
                .filter(link->query.containsItem(link.toJson(this,1,0)))
                .map(NamedRepositoryObject.class::cast);
            Stream<NamedRepositoryObject> workspaces = hasDocumentId 
                ? Stream.empty() 
                : db.getFolders(path, query, DatabaseInterface.GET_WORKSPACE)
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
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {            
            Stream<Document> docs = db.getDocuments(Id.ofDocument(reference.id), query, DatabaseInterface.GET_DOCUMENT)
                .filter(link->query.containsItem(link.toJson(this,1,0)));
            return LOG.exit(docs);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public NamedRepositoryObject getObjectByName(RepositoryPath path, Options.Get... options) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(path);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Info info = db.getInfo(path,DatabaseInterface.GET_INFO).orElseThrow(()->new Exceptions.InvalidObjectName(path));
            RepositoryPath shortPath = RepositoryPath.ROOT.addId(info.id.toString());
            switch(info.type) {
                case WORKSPACE:
                    return LOG.exit(db.getFolder(shortPath, DatabaseInterface.GET_WORKSPACE)
                        .orElseThrow(()->new Exceptions.InvalidObjectName(shortPath)));
                case DOCUMENT_LINK:
                    return LOG.exit(db.getDocumentLink(shortPath, DatabaseInterface.GET_LINK)
                        .orElseThrow(()->new Exceptions.InvalidObjectName(shortPath)));
                default:
                    throw LOG.throwing(new RuntimeException("Don't know how to get " + info.type));
            }

        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace getWorkspaceByName(RepositoryPath path) throws Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName {
        LOG.entry(path);
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Workspace workspace = db.getFolder(path, DatabaseInterface.GET_WORKSPACE)
                .orElseThrow(doThrowInvalidWorkspace(path));
            return LOG.exit(workspace);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public InputStream getData(Reference rfrnc, RepositoryPath partName) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName, IOException {
        if (!partName.isEmpty()) throw new UnsupportedOperationException("Doesn't support a part name");
        return filestore.get(filestore.parseKey(rfrnc.version));
    }

    @Override
    public void writeData(Reference rfrnc, RepositoryPath partName, OutputStream out) throws Exceptions.InvalidReference, Exceptions.InvalidObjectName, IOException {
        if (!partName.isEmpty()) throw new UnsupportedOperationException("Doesn't support a part name");
        OutputStreamConsumer.of(()->filestore.get(filestore.parseKey(rfrnc.version))).consume(out);
    }
    
    @Override
    public DocumentLink publishDocumentLink(RepositoryPath path, String name, JsonObject metadata) throws Exceptions.InvalidObjectName, Exceptions.InvalidVersionName {
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Info info = db.getInfo(path, DatabaseInterface.GET_INFO).orElseThrow(()->LOG.throwing(new Exceptions.InvalidObjectName(path)));
            if (info.type != RepositoryObject.Type.DOCUMENT_LINK) throw LOG.throwing(new Exceptions.InvalidObjectName(path));
            RepositoryPath published = RepositoryPath.ROOT.addId(db.publish(info.id, name).toString());
            DocumentLink result = db.getDocumentLink(published, DatabaseInterface.GET_LINK).orElseThrow(()->new RuntimeException("Failed to publish"));
            db.commit();
            return LOG.exit(result);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Workspace publishWorkspace(RepositoryPath path, String name, JsonObject metadata) throws Exceptions.InvalidWorkspace, Exceptions.InvalidVersionName {
        try (
            DatabaseInterface db = dbFactory.getInterface(); 
        ) {
            Info info = db.getInfo(path, DatabaseInterface.GET_INFO).orElseThrow(()->LOG.throwing(new Exceptions.InvalidWorkspace(path)));
            if (info.type != RepositoryObject.Type.WORKSPACE) throw LOG.throwing(new Exceptions.InvalidWorkspace(path));
            RepositoryPath published = RepositoryPath.ROOT.addId(db.publish(info.id, name).toString());
            Workspace result = db.getFolder(published, DatabaseInterface.GET_WORKSPACE).orElseThrow(()->new RuntimeException("Failed to publish"));
            db.commit();
            return LOG.exit(result);
        } catch (SQLException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }    
}
