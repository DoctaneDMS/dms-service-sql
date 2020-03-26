/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 *
 * @author jonathan
 */
public class LocalFilesystem implements Filestore<Id> {
    
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
        try {
            return Files.newInputStream(toPath(key));    
        } catch (IOException e) {
            throw new NotFound(key);
        }
    }

    @Override
    public void put(Id version, InputStreamSupplier iss) {
        Path path = toPath(version);
        try (InputStream is = iss.get()) {
            Files.createDirectories(path.getParent());
            Files.copy(is, path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void link(Id from, Id to) throws NotFound {
        Path toPath = toPath(to);
        try {
            Files.createDirectories(toPath.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            Files.createLink(toPath, toPath(from));
        } catch (IOException e) {
            throw new NotFound(from);
        }
    }
    
    @Override
    public void remove(Id key) throws NotFound {
        Path path = toPath(key);
        try {
            Files.delete(path);        
        } catch (IOException e) {
            throw new NotFound(key);
        } 
    }
}
