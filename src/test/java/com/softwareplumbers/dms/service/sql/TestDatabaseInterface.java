/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.abstractquery.visitor.Visitors.ParameterizedSQL;
import com.softwareplumbers.common.sql.Schema;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Exceptions.InvalidObjectName;
import com.softwareplumbers.dms.Exceptions.InvalidWorkspace;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.Workspace;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestDatabaseInterface {
    
    @Autowired
    DocumentDatabase factory;
    
    @Autowired
    Schema schema;
    
    @Before
    public void createSchema() throws SQLException {
        schema.dropSchema();        
        schema.createSchema();        
        schema.updateSchema();        
    }

    public static final RepositoryPath TEST_PATH_0 = RepositoryPath.valueOf("~id");    
    public static final RepositoryPath TEST_PATH_1 = RepositoryPath.valueOf("one");
    public static final RepositoryPath TEST_PATH_2 = RepositoryPath.valueOf("one/two");
    public static final RepositoryPath TEST_PATH_3 = RepositoryPath.valueOf("one/two/three");
    public static final RepositoryPath TEST_PATH_V1 = RepositoryPath.valueOf("one@v1");
    public static final RepositoryPath TEST_PATH_V2 = RepositoryPath.valueOf("one/two@v1");
    public static final RepositoryPath TEST_PATH_V3 = RepositoryPath.valueOf("one/two/three@v1");
    public static final RepositoryPath TEST_PATH_ID0 = RepositoryPath.valueOf("~id/~id2");    
    public static final RepositoryPath TEST_PATH_ID1 = RepositoryPath.valueOf("one/~id1");
        
    @Test 
    public void testGetDocumentLinkSQL() throws SQLException {
        try (DatabaseInterface api = factory.getInterface()) {
            ParameterizedSQL l0 = api.getDocumentLinkSQL(TEST_PATH_1);
            System.out.println(l0.sql);
            assertTrue(l0.sql.contains("? || '/' || T0.NAME || CASEWHEN(T0.VERSION='', CASEWHEN(T0.CURRENT, '', '@~' || T0.VERSION_ID), '@' || T0.VERSION) AS PATH"));
            assertTrue(l0.sql.contains("VIEW_LINKS T0"));
            assertTrue(l0.sql.contains("WHERE T0.CURRENT=true AND T0.NAME=? AND T0.PARENT_ID=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("path", l0.parameters[1]);
            assertEquals("parent.path", l0.parameters[2]);
            assertEquals("path.version", l0.parameters[3]);
            ParameterizedSQL l1 = api.getDocumentLinkSQL(TEST_PATH_2);
            System.out.println(l1.sql);
            assertTrue(l1.sql.contains("? || '/' || T1.NAME || CASEWHEN(T1.VERSION='', '', '@' || T1.VERSION) || '/' || T0.NAME || CASEWHEN(T0.VERSION='', CASEWHEN(T0.CURRENT, '', '@~' || T0.VERSION_ID), '@' || T0.VERSION) AS PATH"));
            assertTrue(l1.sql.contains("VIEW_LINKS T0 INNER JOIN VIEW_FOLDERS T1 ON T0.PARENT_ID = T1.ID"));
            assertTrue(l1.sql.contains("WHERE T0.CURRENT=true AND T0.NAME=? AND T1.NAME=? AND T1.PARENT_ID=? AND T1.VERSION=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("path", l1.parameters[1]);
            assertEquals("parent.path", l1.parameters[2]);
            assertEquals("parent.parent.path", l1.parameters[3]);
            assertEquals("parent.path.version", l1.parameters[4]);
            assertEquals("path.version", l1.parameters[5]);
            ParameterizedSQL l2 = api.getDocumentLinkSQL(TEST_PATH_3);
            System.out.println(l2.sql);
            assertTrue(l2.sql.contains("? || '/' || T2.NAME || CASEWHEN(T2.VERSION='', '', '@' || T2.VERSION) || '/' || T1.NAME || CASEWHEN(T1.VERSION='', '', '@' || T1.VERSION) || '/' || T0.NAME || CASEWHEN(T0.VERSION='', CASEWHEN(T0.CURRENT, '', '@~' || T0.VERSION_ID), '@' || T0.VERSION) AS PATH"));
            assertTrue(l2.sql.contains("VIEW_LINKS T0 INNER JOIN VIEW_FOLDERS T1 ON T0.PARENT_ID = T1.ID INNER JOIN VIEW_FOLDERS T2 ON T1.PARENT_ID = T2.ID"));
            assertTrue(l2.sql.contains("WHERE T0.CURRENT=true AND T0.NAME=? AND T1.NAME=? AND T2.NAME=? AND T2.PARENT_ID=? AND T2.VERSION=? AND T1.VERSION=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("path", l2.parameters[1]);
            assertEquals("parent.path", l2.parameters[2]);
            assertEquals("parent.parent.path", l2.parameters[3]);
            assertEquals("parent.parent.parent.path", l2.parameters[4]);
            assertEquals("parent.parent.path.version", l2.parameters[5]);
            assertEquals("parent.path.version", l2.parameters[6]);
            assertEquals("path.version", l2.parameters[7]);
        }
    }
      
    @Test 
    public void testGetDocumentLinkByIdSQL() throws SQLException {
        try (DatabaseInterface api = factory.getInterface()) {
            ParameterizedSQL l0 = api.getDocumentLinkSQL(TEST_PATH_ID0);
            System.out.println(l0.sql);
            assertTrue(l0.sql.contains("? || '/' || T0.NAME || CASEWHEN(T0.VERSION='', CASEWHEN(T0.CURRENT, '', '@~' || T0.VERSION_ID), '@' || T0.VERSION) AS PATH"));
            assertTrue(l0.sql.contains("VIEW_LINKS T0"));
            assertTrue(l0.sql.contains("WHERE T0.CURRENT=true AND T0.PARENT_ID=? AND T0.DOCUMENT_ID=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("parent.path", l0.parameters[1]);
            assertEquals("path", l0.parameters[2]);
            assertEquals("path.version", l0.parameters[3]);
            ParameterizedSQL l1 = api.getDocumentLinkSQL(TEST_PATH_ID1);
            System.out.println(l1.sql);
            assertTrue(l1.sql.contains("? || '/' || T1.NAME || CASEWHEN(T1.VERSION='', '', '@' || T1.VERSION) || '/' || T0.NAME || CASEWHEN(T0.VERSION='', CASEWHEN(T0.CURRENT, '', '@~' || T0.VERSION_ID), '@' || T0.VERSION) AS PATH"));
            assertTrue(l1.sql.contains("VIEW_LINKS T0 INNER JOIN VIEW_FOLDERS T1 ON T0.PARENT_ID = T1.ID"));
            assertTrue(l1.sql.contains("WHERE T0.CURRENT=true AND T1.NAME=? AND T1.PARENT_ID=? AND T1.VERSION=? AND T0.DOCUMENT_ID=? AND T0.VERSION=?"));
            assertEquals("basePath", l1.parameters[0]);
            assertEquals("parent.path", l1.parameters[1]);
            assertEquals("parent.parent.path", l1.parameters[2]);
            assertEquals("parent.path.version", l1.parameters[3]);
            assertEquals("path", l1.parameters[4]);
            assertEquals("path.version", l1.parameters[5]);
        }
    }
    
    @Test 
    public void testGetFolderSQL() throws SQLException {
        try (DatabaseInterface api = factory.getInterface()) {
            ParameterizedSQL l0 = api.getFolderSQL(TEST_PATH_1);
            System.out.println(l0.sql);
            assertTrue(l0.sql.contains("? || '/' || T0.NAME || CASEWHEN(T0.VERSION='', '', '@' || T0.VERSION) AS PATH"));
            assertTrue(l0.sql.contains("VIEW_FOLDERS T0"));
            assertTrue(l0.sql.contains("WHERE T0.NAME=? AND T0.PARENT_ID=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("path", l0.parameters[1]);
            assertEquals("parent.path", l0.parameters[2]);
            assertEquals("path.version", l0.parameters[3]);
            ParameterizedSQL l1 = api.getFolderSQL(TEST_PATH_2);
            System.out.println(l1.sql);
            assertTrue(l1.sql.contains("? || '/' || T1.NAME || CASEWHEN(T1.VERSION='', '', '@' || T1.VERSION) || '/' || T0.NAME || CASEWHEN(T0.VERSION='', '', '@' || T0.VERSION) AS PATH"));
            assertTrue(l1.sql.contains("VIEW_FOLDERS T0 INNER JOIN VIEW_FOLDERS T1 ON T0.PARENT_ID = T1.ID"));
            assertTrue(l1.sql.contains("WHERE T0.NAME=? AND T1.NAME=? AND T1.PARENT_ID=? AND T1.VERSION=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("path", l1.parameters[1]);
            assertEquals("parent.path", l1.parameters[2]);
            assertEquals("parent.parent.path", l1.parameters[3]);
            assertEquals("parent.path.version", l1.parameters[4]);
            assertEquals("path.version", l1.parameters[5]);
            ParameterizedSQL l2 = api.getFolderSQL(TEST_PATH_V3);
            System.out.println(l2.sql);
            assertTrue(l2.sql.contains("? || '/' || T2.NAME || CASEWHEN(T2.VERSION='', '', '@' || T2.VERSION) || '/' || T1.NAME || CASEWHEN(T1.VERSION='', '', '@' || T1.VERSION) || '/' || T0.NAME || CASEWHEN(T0.VERSION='', '', '@' || T0.VERSION) AS PATH"));
            assertTrue(l2.sql.contains("VIEW_FOLDERS T0 INNER JOIN VIEW_FOLDERS T1 ON T0.PARENT_ID = T1.ID INNER JOIN VIEW_FOLDERS T2 ON T1.PARENT_ID = T2.ID"));
            assertTrue(l2.sql.contains("WHERE T0.NAME=? AND T1.NAME=? AND T2.NAME=? AND T2.PARENT_ID=? AND T2.VERSION=? AND T1.VERSION=? AND T0.VERSION=?"));
            assertEquals("basePath", l0.parameters[0]);
            assertEquals("path", l2.parameters[1]);
            assertEquals("parent.path", l2.parameters[2]);
            assertEquals("parent.parent.path", l2.parameters[3]);
            assertEquals("parent.parent.parent.path", l2.parameters[4]);
            assertEquals("parent.parent.path.version", l2.parameters[5]);
            assertEquals("parent.path.version", l2.parameters[6]);
            assertEquals("path.version", l2.parameters[7]);
        }
    }
    
    @Test
    public void testGetInfoSQL() throws SQLException {
        try (DatabaseInterface api = factory.getInterface()) {
            ParameterizedSQL l0 = api.getInfoSQL(TEST_PATH_0);
            System.out.println(l0.sql);
            assertTrue(l0.sql.contains("NODES T0"));
            assertTrue(l0.sql.contains("WHERE T0.ID=?"));
            assertEquals("path", l0.parameters[0]);
            ParameterizedSQL l1 = api.getInfoSQL(TEST_PATH_1);
            System.out.println(l1.sql);
            assertTrue(l1.sql.contains("NODES T0"));
            assertTrue(l1.sql.contains("WHERE T0.NAME=? AND T0.PARENT_ID=? AND T0.VERSION=?"));
            assertEquals("path", l1.parameters[0]);
            assertEquals("parent.path", l1.parameters[1]);
            assertEquals("path.version", l1.parameters[2]);
        }
    }

    @Test
    public void testCreateAndGetDocument() throws SQLException, IOException {
        try (DatabaseInterface api = factory.getInterface()) {
            Id id = new Id();
            Id version = new Id();
            api.createDocument(id, version, "type", 0, "test".getBytes(), JsonValue.EMPTY_JSON_OBJECT);
            api.commit();
            Optional<Document> result = api.getDocument(id, version, DatabaseInterface.GET_DOCUMENT);
            assertTrue(result.isPresent());
            assertEquals(id.toString(), result.get().getReference().id);
            assertEquals(version.toString(), result.get().getReference().version);
            assertEquals(0, result.get().getLength());
            assertArrayEquals("test".getBytes(), result.get().getDigest());
            assertEquals(JsonValue.EMPTY_JSON_OBJECT, result.get().getMetadata());
        }
    }
    
    @Test
    public void testCreateAndGetFolder() throws SQLException, InvalidWorkspace, IOException {
        try (DatabaseInterface api = factory.getInterface()) {
            Id id = api.createFolder(Id.ROOT_ID, "foldername", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            api.commit();
            Optional<Workspace> result = api.getFolder(RepositoryPath.valueOf("foldername"), DatabaseInterface.GET_WORKSPACE);
            assertTrue(result.isPresent());
            assertEquals(id.toString(), result.get().getId());
            assertEquals(Workspace.State.Open, result.get().getState());
            assertEquals(JsonValue.EMPTY_JSON_OBJECT, result.get().getMetadata());
            // Test we can also get folder via Id
            Optional<Workspace> result2 = api.getFolder(RepositoryPath.ROOT.addId(id.toString()), DatabaseInterface.GET_WORKSPACE);
            assertTrue(result2.isPresent());
            assertEquals(id.toString(), result2.get().getId());
            assertEquals(Workspace.State.Open, result2.get().getState());
            assertEquals(JsonValue.EMPTY_JSON_OBJECT, result2.get().getMetadata());
        }
    }
    
    @Test
    public void testUpdateFolder() throws SQLException, IOException, InvalidWorkspace {
        try (DatabaseInterface api = factory.getInterface()) {
            Id id = api.createFolder(Id.ROOT_ID, "foldername", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            api.commit();
            Workspace result = api.updateFolder(id, Workspace.State.Closed, Json.createObjectBuilder().add("test", "hello").build(), DatabaseInterface.GET_WORKSPACE).get();
            assertEquals(id.toString(), result.getId());
            assertEquals(Workspace.State.Closed, result.getState());
            assertEquals("hello", result.getMetadata().getString("test"));
        }
    }
    
    @Test
    public void testCreateAndGetFolderWithPath() throws InvalidWorkspace, SQLException, IOException {
        try (DatabaseInterface api = factory.getInterface()) {
            Id parent_id = api.createFolder(Id.ROOT_ID, "parent", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id child_id = api.createFolder(parent_id, "child", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id grandchild_id = api.createFolder(child_id, "grandchild", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            api.commit();
            Optional<Workspace> result = api.getFolder(RepositoryPath.ROOT.add("parent","child","grandchild"), DatabaseInterface.GET_WORKSPACE);
            assertTrue(result.isPresent());
            assertEquals(grandchild_id.toString(), result.get().getId());
            assertEquals(RepositoryPath.valueOf("parent/child/grandchild"), result.get().getName());
        }
    }
    
    @Test
    public void testCopyFolderWithPath() throws SQLException, IOException, InvalidObjectName, InvalidWorkspace {
        try (DatabaseInterface api = factory.getInterface()) {
            Id parent_id = api.createFolder(Id.ROOT_ID, "parent", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id child_id = api.createFolder(parent_id, "child", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            api.createFolder(child_id, "grandchild", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            RepositoryPath parentPath = RepositoryPath.ROOT.addId(parent_id.toString());
            Workspace sibling = api.copyFolder(parentPath.add("child"), parentPath.add("sibling"), false, DatabaseInterface.GET_WORKSPACE);
            assertEquals(RepositoryPath.valueOf("parent/sibling"), sibling.getName());
            Optional<Workspace> cousin = api.getFolder(RepositoryPath.ROOT.add("parent","sibling","grandchild"), DatabaseInterface.GET_WORKSPACE);
            assertTrue(cousin.isPresent());
        }
    }
    
    @Test
    public void testCopyDocumentLinkWithPath() throws SQLException, IOException, InvalidObjectName, InvalidWorkspace {
        try (DatabaseInterface api = factory.getInterface()) {
            Id parent_id = api.createFolder(Id.ROOT_ID, "parent", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id child_id = api.createFolder(parent_id, "child", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id id = new Id();
            Id version = new Id();
            api.createDocument(id, version, "type", 0, "test".getBytes(), JsonValue.EMPTY_JSON_OBJECT);
            api.createDocumentLink(child_id, "grandchild", id, version, DatabaseInterface.GET_ID);
            RepositoryPath childPath = RepositoryPath.ROOT.addId(child_id.toString());
            RepositoryPath parentPath = RepositoryPath.ROOT.addId(parent_id.toString());
            DocumentLink sibling = api.copyDocumentLink(childPath.add("grandchild"), parentPath.add("sibling"), false, DatabaseInterface.GET_LINK);
            assertEquals(RepositoryPath.valueOf("parent/sibling"), sibling.getName());
            assertEquals(id.toString(), sibling.getReference().id);
            assertEquals(version.toString(), sibling.getReference().version);
        }
    }
    
    @Test
    public void testCreateAndGetDocumentLink() throws SQLException, IOException, InvalidWorkspace {
        try (DatabaseInterface api = factory.getInterface()) {
            Id folder_id = api.createFolder(Id.ROOT_ID, "foldername", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id id = new Id();
            Id version = new Id();
            api.createDocument(id, version, "type", 0, "test".getBytes(), JsonValue.EMPTY_JSON_OBJECT);
            api.createDocumentLink(folder_id, "docname", id, version, DatabaseInterface.GET_ID);
            api.commit();
            Optional<DocumentLink> result = api.getDocumentLink(RepositoryPath.ROOT.addId(folder_id.toString()).add("docname"), DatabaseInterface.GET_LINK);
            assertTrue(result.isPresent());
            assertEquals(RepositoryPath.valueOf("foldername/docname"), result.get().getName());
            assertEquals(id.toString(), result.get().getReference().id);
            assertEquals(version.toString(), result.get().getReference().version);
            assertEquals("type", result.get().getMediaType());
            assertEquals(0, result.get().getLength());
            assertArrayEquals("test".getBytes(), result.get().getDigest());
            assertEquals(JsonValue.EMPTY_JSON_OBJECT, result.get().getMetadata());
        }
    }

    @Test
    public void testCreateAndGetDocumentLinkWithPath() throws SQLException, IOException, InvalidWorkspace {
        try (DatabaseInterface api = factory.getInterface()) {
            Id folder_id = api.createFolder(Id.ROOT_ID, "foldername", Workspace.State.Open, JsonValue.EMPTY_JSON_OBJECT, DatabaseInterface.GET_ID);
            Id id = new Id();
            Id version = new Id();
            api.createDocument(id, version, "type", 0, "test".getBytes(), JsonValue.EMPTY_JSON_OBJECT);
            api.createDocumentLink(folder_id, "docname", id, version, DatabaseInterface.GET_ID);
            api.commit();
            Optional<DocumentLink> result = api.getDocumentLink(RepositoryPath.ROOT.add("foldername","docname"), DatabaseInterface.GET_LINK);
            assertTrue(result.isPresent());
            assertEquals(RepositoryPath.valueOf("foldername/docname"), result.get().getName());
            assertEquals(id.toString(), result.get().getReference().id);
            assertEquals(version.toString(), result.get().getReference().version);
            assertEquals("type", result.get().getMediaType());
            assertEquals(0, result.get().getLength());
            assertArrayEquals("test".getBytes(), result.get().getDigest());
            assertEquals(JsonValue.EMPTY_JSON_OBJECT, result.get().getMetadata());
        }
    }    
}