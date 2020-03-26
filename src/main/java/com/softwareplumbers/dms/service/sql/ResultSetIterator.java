/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * @author jonathan
 */
public class ResultSetIterator<T> implements Iterator<T>, AutoCloseable {
    private ResultSet rs;
    private Mapper<T> mapper;
    private T result;
    boolean hasNext;
    
    private final void moveNext() {
        try {
            hasNext = rs.next();
            if (hasNext) result = mapper.map(rs);
        } catch (SQLException e) {
            hasNext = false;
            throw new RuntimeException(e);
        }
    }
    
    public ResultSetIterator(ResultSet rs, Mapper<T> mapper) {
        this.rs = rs;
        this.mapper = mapper;
        moveNext();
    }

    @Override
    public T next() {
        T rv = result;
        moveNext();
        return rv;
    }
    
    @Override
    public boolean hasNext() {
        return hasNext;
    }
    
    @Override
    public void close() {
        try {
            rs.close();
        } catch (SQLException e) {
            // DNGAS
        }
    }
    
    Stream<T> toStream() {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(this, 0);
        return StreamSupport.stream(spliterator, false).onClose(this::close);
    }
    
}
