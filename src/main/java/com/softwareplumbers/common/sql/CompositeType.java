/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.common.sql;

/**
 *
 * @author jonathan
 */
@FunctionalInterface
public interface CompositeType<T> {
    FluentStatement set(FluentStatement statement, String name, T type);
}
