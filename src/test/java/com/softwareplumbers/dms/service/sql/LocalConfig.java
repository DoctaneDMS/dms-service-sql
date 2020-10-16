package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.sql.AbstractDatabase;
import com.softwareplumbers.common.sql.DatabaseConfig;
import com.softwareplumbers.common.sql.DatabaseConfigFactory;
import com.softwareplumbers.common.sql.OperationStore;
import com.softwareplumbers.common.sql.Schema;
import com.softwareplumbers.common.sql.TemplateStore;
import com.softwareplumbers.dms.common.test.TestModel;
import static com.softwareplumbers.dms.service.sql.DocumentDatabase.*;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.ApplicationContext;
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
@ImportResource({"classpath:com/softwareplumbers/dms/service/sql/entities.xml", "classpath:com/softwareplumbers/dms/service/sql/mysqldb.xml", "classpath:com/softwareplumbers/dms/service/sql/h2db.xml"})
public class LocalConfig {
    
    @Autowired
    Environment env;
       
    @Autowired
    ApplicationContext context;
    
    @Bean public Filestore filestore() {
        return new LocalFilesystem(Paths.get(env.getProperty("installation.root")).resolve("documents"));
    }
    
    @Bean
    public DatabaseConfigFactory<EntityType, DataType, Operation, Template> configFactory() {
        return variant-> {
            switch(variant) {
                case H2: return context.getBean("h2.dms.config", DatabaseConfig.class);
                case MYSQL: return context.getBean("mysql.dms.config", DatabaseConfig.class);
                default: throw new RuntimeException("Unhandled variant " + variant);
            }
        };                  
    }    
    
    public Properties dbCredentials() {
        Properties credentials = new Properties();
        credentials.put("username", env.getProperty("database.user"));
        credentials.put("password", env.getProperty("database.password"));
        return credentials;
    }
    
    @Bean public DocumentDatabase database(DatabaseConfigFactory<EntityType, DataType, Operation, Template> config) throws SQLException {
        return new DocumentDatabase(URI.create(env.getProperty("database.url")), dbCredentials(), config, CreateOption.RECREATE);
    }
   
    @Bean public SQLRepositoryService service(DocumentDatabase database, Filestore filestore) throws SQLException {
        return new SQLRepositoryService(database, filestore);
    }
     
    @Bean(name="dms.datasource") public DataSource datasource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(env.getProperty("database.driver"));
        dataSourceBuilder.url(env.getProperty("database.url"));
        dataSourceBuilder.username(env.getProperty("database.user"));
        dataSourceBuilder.password(env.getProperty("database.password"));
        return dataSourceBuilder.build();        
    }
    
    @Bean public SQLRepositoryServiceMBean mbean(DocumentDatabase database, Filestore files) {
        return new SQLRepositoryServiceMBean(database, files);
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
