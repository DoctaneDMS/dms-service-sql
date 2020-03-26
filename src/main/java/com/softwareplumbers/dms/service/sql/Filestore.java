/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import java.io.IOException;
import java.io.InputStream;

/** Abstraction of simple file storage system.
 * 
 * A filestore is a persistent map which allows binary data to
 * be stored and accessed via a key.
 * 
 * In addition to this basic functionality, it provides a way to create a 'link'
 * associating one file with multiple Ids.
 *
 * @author Jonathan Essex
 */
public interface Filestore<K> {
    
    /** 'Not Found' error thrown when a key is not valid 
     */
    public static class NotFound extends IOException {
        
        /** Construct a new NotFound exception.
         * 
         * @param key The key which cannot be found.
         */
        public NotFound(Object key) { super("File not found: " + key); }
    }
    
    /** Parse a key from a string.
     * 
     * Note that this should be consistent with the implementation of the 
     * key type's toString method such that for a key k,
     * ```
     * k.equals(this.parseKey(k.toString()))
     * ```
     * @param key String representation of a key
     * @return Native key representation.
     */
    K parseKey(String key);
    
    /** Generate a new key value.
     * 
     * Key value should be unique within the Filestore.
     * 
     * @return  A new key value.
     */
    K generateKey();
    
    /** Get the binary data associated with the key.
     * 
     * It is the caller's responsibility to close the input stream.
     * 
     * @param key
     * @return A stream of binary data representing the file referenced by key.
     * @throws NotFound if key value not previously put or linked. 
     */
    InputStream get(K key) throws NotFound;
    
    /** Associate binary data with some key value.
     * 
     * @param key
     * @param data 
     */
    void put(K key, InputStreamSupplier data);
    
    /** Link previously stored binary data with a new key value.
     * 
     * The old key value remains associated with the binary data after this
     * operation completes; get(from) is still valid.  
     * 
     * @param from Old key value with previously stored data
     * @param to New key value to associate with the same stored data
     * @throws NotFound if from does not have any associated data.
     */
    void link(K from, K to) throws NotFound;
    
    /** Remove the association between a key and its data
     * 
     * @param key
     * @throws NotFound if key does not have any associated data.
     */
    void remove(K key) throws NotFound;
}
