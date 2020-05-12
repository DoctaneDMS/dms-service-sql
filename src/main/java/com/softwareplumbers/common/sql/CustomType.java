/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.common.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * @author jonathan
 */
@FunctionalInterface
public interface CustomType<T> {
    void set(PreparedStatement statement, int index, T value) throws SQLException;
}
