/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 * @author jonat
 */
public class TestPathProperty {
    
    @Test
    public void testLoadXmlConfig() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("/testPathProperty.xml");
        LocalFilesystem lfs = context.getBean(LocalFilesystem.class);
    }
    
}
