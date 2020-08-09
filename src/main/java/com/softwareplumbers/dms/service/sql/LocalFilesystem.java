/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.common.impl.StreamInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public class LocalFilesystem implements Filestore<Id> {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(LocalFilesystem.class);
    
    private Path basePath;
    
    private Path toPath(Id id) {
        Path path = basePath;
        for (String elem : id.toString().split("-")) {
            path = path.resolve(elem);
        }
        return path;
    }
    
    public LocalFilesystem(Path basePath) {
        this.basePath = basePath;
    }
    
    public LocalFilesystem() {
        this(Paths.get("/var/tmp/doctane/filestore"));
    }
    
    /** Set path from an array of path segments.
     * 
     * We use this in spring XML configuration so we can create a path from separate elements
     * in a platform-independent way.
     * 
     * @param basePath 
     */
    public void setPathParts(String[] basePath) {
        LOG.entry(Arrays.asList(basePath));
        this.basePath = Paths.get(basePath[0]);
        for (int i = 1; i < basePath.length; i++) {
            this.basePath = this.basePath.resolve(basePath[i]);
        }
        LOG.exit();
    }
    
    public void setPath(String basePath) {
        LOG.entry(basePath);
        this.basePath = Paths.get(basePath);
        LOG.exit();
    }

    @Override
    public Id parseKey(String key) {
        return Id.ofVersion(key);
    }
    
    @Override 
    public Id generateKey() {
        return new Id();
    }

    @Override
    public InputStream get(Id key) throws NotFound {
        LOG.entry(key);
        try {
            return LOG.exit(Files.newInputStream(toPath(key)));    
        } catch (IOException e) {
            throw LOG.throwing(new NotFound(key));
        }
    }

    @Override
    public void put(Id version, Document document, StreamInfo iss) {
        LOG.entry(version, document, "<stream>");
        Path path = toPath(version);
        try (InputStream is = iss.supplier.get()) {
            Files.createDirectories(path.getParent());
            Files.copy(is, path);
        } catch (IOException e) {
            throw LOG.throwing(new UncheckedIOException(e));
        }
        LOG.exit();
    }

    @Override
    public void link(Document document, Id from, Id to) throws NotFound {
        LOG.entry(document, from, to);
        Path toPath = toPath(to);
        try {
            Files.createDirectories(toPath.getParent());
        } catch (IOException e) {
            throw LOG.throwing(new UncheckedIOException(e));
        }
        try {
            Files.createLink(toPath, toPath(from));
        } catch (IOException e) {
            throw LOG.throwing(new NotFound(from));
        }
        LOG.exit();
    }
    
    @Override
    public void remove(Id key) throws NotFound {
        LOG.entry(key);
        Path path = toPath(key);
        try {
            Files.delete(path);        
        } catch (IOException e) {
            throw LOG.throwing(new NotFound(key));
        } 
        LOG.exit();
    }
}
