package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.sql.FluentStatement;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Options;
import java.util.UUID;

import org.junit.Before;

import com.softwareplumbers.dms.common.test.BaseRepositoryServiceTest;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.common.impl.StreamInfo;
import com.softwareplumbers.dms.common.test.TestModel;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;
import javax.json.JsonValue;
import org.junit.After;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class SQLRepositoryServiceTest extends BaseRepositoryServiceTest {
	
    @Autowired
	public SQLRepositoryService service;
    
    @Autowired
    DocumentDatabase factory;
    
    @Autowired
    Filestore<Id> filestore;
    
    int connectionCount = 0;

	@Override
	public RepositoryService service() {
		return service;
	}
    
    @Autowired @Qualifier("workspaceMetadataModel")
    TestModel workspaceMetadataModel;
    
    @Autowired @Qualifier("documentMetadataModel")
    TestModel documentMetadataModel;
    
    @Override
    public Reference randomDocumentReference() {
        return new Reference(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    
    @Override
    public String randomWorkspaceId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public TestModel documentMetadataModel() {
        return documentMetadataModel;
    }
    
    @Override
    public TestModel workspaceMetadataModel() {
        return workspaceMetadataModel;
    }
    
    @Before
    public void setCountBefore() {
        connectionCount = FluentStatement.getConnectionCount();
    }

    @After
    public void setCountAfter() {
        assertEquals(connectionCount, FluentStatement.getConnectionCount());
    }
    
    @Test
    public void testExternalReferenceUpdatesMetadata() throws SQLException, IOException, Exceptions.InvalidObjectName, Exceptions.InvalidReference, Exceptions.InvalidWorkspace, Exceptions.InvalidWorkspaceState {
        
        Id version = filestore.generateKey();
        InputStreamSupplier data = InputStreamSupplier.markPersistent(()->new ByteArrayInputStream("test123".getBytes()));
        StreamInfo streamInfo = StreamInfo.of(data);
        filestore.put(version, null, streamInfo);
        Id id = new Id();
        try (DatabaseInterface api = factory.getInterface()) {
            byte[] wrongDigest = new byte[32];
            new Random().nextBytes(wrongDigest);        
            api.createDocument(id, version, "text/plain", streamInfo.length, wrongDigest, JsonValue.EMPTY_JSON_OBJECT);
            api.commit();
        }
        
        DocumentLink link = service().createDocumentLink(RepositoryPath.ROOT.add("directory","hello.txt"), new Reference(id.toString(), version.toString()), Options.EXTERNAL_REFERENCE, Options.CREATE_MISSING_PARENT);
        
        assertArrayEquals(streamInfo.digest, link.getDigest());
    }

}
