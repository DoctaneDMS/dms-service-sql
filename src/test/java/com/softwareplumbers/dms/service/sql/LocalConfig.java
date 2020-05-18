package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.sql.OperationStore;
import com.softwareplumbers.common.sql.Schema;
import com.softwareplumbers.common.sql.TemplateStore;
import com.softwareplumbers.dms.common.test.TestModel;
import java.nio.file.Paths;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author jonathan
 */
@ImportResource({"classpath:com/softwareplumbers/dms/service/sql/h2db.xml","classpath:com/softwareplumbers/dms/service/sql/entities.xml"})
public class LocalConfig {
    
    @Autowired
    OperationStore<DocumentDatabase.Operation> operations;
    
    @Autowired
    TemplateStore<DocumentDatabase.Template> templates;
    
    @Autowired
    Schema schema;

    @Bean public Filestore filestore() {
        return new LocalFilesystem(Paths.get("/var/tmp/doctane/filestore"));
    }
    
    @Bean public DocumentDatabase database() {
        DocumentDatabase database = new DocumentDatabase(schema);
        database.setOperations(operations);
        database.setTemplates(templates);
        return database;
    }
    
    @Bean public SQLRepositoryService service() throws SQLException {
        return new SQLRepositoryService(database(), filestore());
    }
     
    @Bean public DataSource datasource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.h2.Driver");
        dataSourceBuilder.url("jdbc:h2:file:/var/tmp/doctane/test");
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("");
        return dataSourceBuilder.build();        
    }
    
    @Bean public TestModel documentMetadataModel() {
        TestModel.Field uniqueField = new TestModel.IdField("DocFaceRef");
        TestModel model = new TestModel(
                new TestModel.StringField("TradeDescription", "BR001", "BR002", "BR003", "BR004"),
                new TestModel.BooleanField("BankDocument"),
                new TestModel.SessionIdField("BatchID"),
                uniqueField
        );
        model.setUniqueField(uniqueField);
        return model;
    }

    @Bean public TestModel workspaceMetadataModel() {
        return new TestModel(
                new TestModel.StringField("EventDescription", "Event01", "Event02", "Event03", "Event04"),
                new TestModel.StringField("Branch", "BR001", "BR002", "BR003", "BR004"),
                new TestModel.SessionIdField("TheirReference")
        );
    }    
}
