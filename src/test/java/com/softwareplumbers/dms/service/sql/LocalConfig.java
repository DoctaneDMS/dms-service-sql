package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.sql.AbstractDatabase;
import com.softwareplumbers.common.sql.OperationStore;
import com.softwareplumbers.common.sql.Schema;
import com.softwareplumbers.common.sql.TemplateStore;
import com.softwareplumbers.dms.common.test.TestModel;
import java.nio.file.Paths;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.env.Environment;

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
    Environment env;
    
    @Bean public Filestore filestore() {
        return new LocalFilesystem(Paths.get(env.getProperty("installation.root")).resolve("documents"));
    }
    
    @Bean public DocumentDatabase database(
        OperationStore<DocumentDatabase.Operation> operations,
        TemplateStore<DocumentDatabase.Template> templates,
        @Qualifier(value="dms.schema") Schema schema
    ) throws SQLException {
        DocumentDatabase database = new DocumentDatabase(schema);
        database.setOperations(operations);
        database.setTemplates(templates);
        database.setCreateOption(AbstractDatabase.CreateOption.RECREATE);
        return database;
    }
    
    @Bean public SQLRepositoryService service(DocumentDatabase database, Filestore filestore) throws SQLException {
        return new SQLRepositoryService(database, filestore);
    }
     
    @Bean(name="dms.datasource") public DataSource datasource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.h2.Driver");
        dataSourceBuilder.url(env.getProperty("database.url"));
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("");
        return dataSourceBuilder.build();        
    }
    
    @Bean public TestModel documentMetadataModel() {
        TestModel.Field uniqueField = new TestModel.IdField("DocFaceRef");
        TestModel model = new TestModel(
                new TestModel.StringField("DocumentTitle", "BillOfLading", "Invoice", "Purchase Order", "Insurance", "Manifest"),
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
