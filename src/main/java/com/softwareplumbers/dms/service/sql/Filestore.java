/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author jonathan
 */
public interface Filestore<K> {
    
    public static class NotFound extends IOException {
        public NotFound(Object key) { super("File not found: " + key); }
    }
    
    K parseKey(String key);
    K generateKey();
    
    InputStream get(K key) throws NotFound;
    void put(K key, InputStreamSupplier data);
    void link(K from, K to) throws NotFound;
    void remove(K key) throws NotFound;
}
