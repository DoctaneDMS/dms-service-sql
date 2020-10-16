/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Options;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.common.test.TestModel;
import static com.softwareplumbers.dms.common.test.TestUtils.randomText;
import static com.softwareplumbers.dms.common.test.TestUtils.randomUrlSafeName;
import static com.softwareplumbers.dms.common.test.TestUtils.toStream;
import static com.softwareplumbers.dms.common.test.TestUtils.randomQualifiedName;
import java.sql.SQLException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author SWPNET\jonessex
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestSQLRepositoryServiceMBean {
    
    @Autowired @Qualifier("workspaceMetadataModel")
    TestModel workspaceMetadataModel;
    
    @Autowired @Qualifier("documentMetadataModel")
    TestModel documentMetadataModel;
    
    @Autowired
    RepositoryService service;
    
    @Autowired
    DocumentDatabase documents;
    
    @Autowired
    Filestore filestore;
    
    @Autowired
    SQLRepositoryServiceMBean mbean;
    
    
    public TestModel documentMetadataModel() { return documentMetadataModel; }
    public TestModel workspaceMetadataModel() { return workspaceMetadataModel; }
    public RepositoryService service() { return service; }
    
    @Test
    public void testPassCount() throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidVersionName {
		Workspace workspace = service().createWorkspaceByName(randomQualifiedName(), Workspace.State.Open, workspaceMetadataModel().generateValue(), Options.CREATE_MISSING_PARENT);
		String originalText = randomText(); 
        String linkName = randomUrlSafeName();
		// Create a document in the workspace
		DocumentLink link1 = workspace.createLink(service(), linkName, "text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());
		// now update the document
        DocumentLink link2 = link1.update(service(), "text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());
        // now publish the workspace
        Workspace published = workspace.publish(service(), "test01");
        // now update the document again
        DocumentLink link3 = link2.update(service(), "text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());        
        
        SQLRepositoryServiceMBean.IntegrityCheckStatus status = new SQLRepositoryServiceMBean.IntegrityCheckStatus();
        mbean.checkIntegrity("", false, status);
        
        assertThat(status.errors, equalTo(0));
        assertThat(status.ok, equalTo(3));
        assertThat(status.fixed, equalTo(0));
        assertThat(status.failed, equalTo(0));
    }

    @Test
    public void testFailedCountAndFix() throws Exceptions.InvalidWorkspaceState, Exceptions.InvalidWorkspace, Exceptions.InvalidObjectName, Exceptions.InvalidVersionName, Exceptions.InvalidReference, SQLException {
		Workspace workspace = service().createWorkspaceByName(randomQualifiedName(), Workspace.State.Open, workspaceMetadataModel().generateValue(), Options.CREATE_MISSING_PARENT);
		String originalText = randomText(); 
        String linkName = randomUrlSafeName();
		// Create a document in the workspace
		DocumentLink link1 = workspace.createLink(service(), linkName, "text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());
		// now update the document
        DocumentLink link2 = link1.update(service(), "text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());
        // Use low-level update to break digest
        try (DatabaseInterface ifc = documents.getInterface()) {
            ifc.updateDigest(link2.getReference(), "garbage".getBytes());
            ifc.commit();
        }
        // now publish the workspace
        Workspace published = workspace.publish(service(), "test01");
        // now update the document again
        DocumentLink link3 = link2.update(service(), "text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());        
        
        SQLRepositoryServiceMBean.IntegrityCheckStatus status = new SQLRepositoryServiceMBean.IntegrityCheckStatus();
        mbean.checkIntegrity(workspace.getName().left(2).toString(), false, status);
        
        assertThat(status.errors, equalTo(0));
        assertThat(status.ok, equalTo(2));
        assertThat(status.fixed, equalTo(0));
        assertThat(status.failed, equalTo(1));
        
        status = new SQLRepositoryServiceMBean.IntegrityCheckStatus();
        mbean.checkIntegrity(workspace.getName().left(2).toString(), true, status);
        
        assertThat(status.errors, equalTo(0));
        assertThat(status.ok, equalTo(2));
        assertThat(status.fixed, equalTo(1));
        assertThat(status.failed, equalTo(1));

        status = new SQLRepositoryServiceMBean.IntegrityCheckStatus();
        mbean.checkIntegrity(workspace.getName().left(2).toString(), true, status);

        assertThat(status.errors, equalTo(0));
        assertThat(status.ok, equalTo(3));
        assertThat(status.fixed, equalTo(0));
        assertThat(status.failed, equalTo(0));
    }
}
