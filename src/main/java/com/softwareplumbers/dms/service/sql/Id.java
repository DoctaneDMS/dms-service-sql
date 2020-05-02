/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.dms.Constants;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 *
 * @author jonathan
 */
public class Id {
    private final byte[] data;
    
    private static final byte[] ROOT_ID_DATA = new byte[] { 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0 };
    public static final Id ROOT_ID = new Id(ROOT_ID_DATA);
    
    public Id(String id) {
        data = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(data);
        UUID uuid = UUID.fromString(id);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
    }
    
    public Id(byte[] data) {
        this.data = Arrays.copyOf(data, data.length);
    }
    
    public Id() {
        data = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(data);
        UUID uuid = UUID.randomUUID();
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());        
    }
        
    public String toString() { 
        ByteBuffer bb = ByteBuffer.wrap(data);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong).toString();
    }
    
    public boolean equals(Id other) {
        return Arrays.equals(data, other.data);
    }
    
    public boolean equals(Object other) {
        return other instanceof Id && equals((Id)other);
    }
    
    public long hashValue() {
        return Arrays.hashCode(data);
    }
    
    byte[] getBytes()  {
        return data;
    }
    
    public static Id of(String string) {
        if (string == Constants.ROOT_ID) return ROOT_ID;
        else return new Id(string);
    }
    
    public static Id of(byte[] bytes) {
        if (bytes == null) return null;
        if (Arrays.equals(bytes, ROOT_ID_DATA)) return ROOT_ID;
        return new Id(bytes);
    }
    
    public static Id ofDocument(String string) {
        return string == null ? null : new Id(string);
    }
    
    public static Id ofVersion(String string) {
        return string == null ? null : new Id(string);
    }

}
