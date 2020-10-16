/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.StreamInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** MBean for admin operations on SQL Repository service.
 *
 * @author jonathan essex
 */
public class SQLRepositoryServiceMBean {
    
    private final XLogger LOG = XLoggerFactory.getXLogger(SQLRepositoryServiceMBean.class);
    private final DocumentDatabase database;
    private final Filestore filestore;
    
    public SQLRepositoryServiceMBean(DocumentDatabase database, Filestore filestore) {
        this.database = database;
        this.filestore = filestore;
    }
    
    static class IntegrityCheckStatus {
        public int ok = 0;
        public int failed = 0;
        public int fixed = 0;
        public int errors = 0;
        @Override
        public String toString() { return String.format("OK: %d, Failed: %d, Fixed: %d, Errors: %d", ok, failed, fixed, errors); }
    }
    
    void checkIntegrity(String path, boolean fix, IntegrityCheckStatus status) {
        LOG.entry(path, fix, status);
        try (DatabaseInterface ifc = database.getInterface()) {            
            try (Stream<DocumentLink> docs = ifc.getDocumentLinks(RepositoryPath.valueOf(path).add("*").setVersion("*"), Query.UNBOUNDED, true, DatabaseInterface.GET_LINK)) {
                docs.forEach(link->{
                    try {
                        LOG.trace("Got link {}", link.getReference());
                        StreamInfo info = StreamInfo.of(()->filestore.get(filestore.parseKey(link.getReference().getVersion())));
                        if (Arrays.equals(info.digest, link.getDigest())) {
                            LOG.info("OK: {}", link.getName());
                            status.ok++;
                        } else {
                            LOG.info("FAILED: {}", link.getName());
                            LOG.info("Actual: {}", link.getDigest());
                            LOG.info("Calculated: {}", info.digest);
                            status.failed++;
                            if (fix) {
                                ifc.updateDigest(link.getReference(), info.digest);
                                ifc.commit();
                                status.fixed++;
                            }
                        }
                    } catch (Exception e) {
                        status.errors++;
                        LOG.catching(e);
                        LOG.error("Error checking integrity ", e);                        
                    }
                });
            }             
            try (Stream<Workspace> docs = ifc.getFolders(RepositoryPath.valueOf(path).add("*"), Query.UNBOUNDED, true, DatabaseInterface.GET_WORKSPACE)) {
                docs.forEach(workspace->{
                    checkIntegrity(path + "/" + workspace.getName().part, fix, status);
                });
            } catch (Exception e) {
                status.errors++;
                LOG.catching(e);
                LOG.error("Error checking integrity ", e);                                        
            }
            
        } catch (SQLException e) {
            status.errors++;
            LOG.catching(e);
            LOG.error("Error checking integrity ", e);
        }
        LOG.exit();
    }
    
    public String checkIntegrity(String path, boolean fix) {
        IntegrityCheckStatus status = new IntegrityCheckStatus();
        checkIntegrity(path, fix, status);
        return status.toString();
    }
}
