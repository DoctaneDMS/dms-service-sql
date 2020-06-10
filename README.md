# Document Management SQL Service Module

This package contains a Doctane repository service build on a generic SQL store for
metadata. Actual document files are stored using a separate service; this package
also contains an implementation of this service which uses the server's local file system
as a document store. 

This service is the _default service_ for the Doctane REST server (rest-server-dms).
When build as a standalone spring boot application with the default configuration,
rest-server-dms will use this service to store documents. See the rest-server-dms
package [here](https://projects.softwareplumbers.com/document-management/rest-server-dms)
for information about installing Doctane REST server.

## Configuration

### services.xml

The services.xml file contains several spring bean definitions for the sql service module. It
is See [Doctane Core](https://projects.softwareplumbers.com/document-management/rest-server-core) 
for information on configuring the core server. Additional configuration related to this 
service module is described below:

Firstly, we must import the database scripts needed to create and the database schema
and the SQL statements necessary to implement common Doctane operations on the database.

```xml    
    <import resource="classpath:/com/softwareplumbers/dms/service/sql/h2db.xml" />
    <import resource="classpath:/com/softwareplumbers/dms/service/sql/entities.xml" />
```  

The standard h2db.xml file should be reasonably compatible with most SQL servers and
can be modified in order to support any SQL dialect. As well as the templated operations
included in the xml configuration above, the SQL service module also generates certain
statements and clauses programatically. This is done in the DocumentDatabase class, which
is configured below:

```xml   
    <bean id="database" class="com.softwareplumbers.dms.service.sql.DocumentDatabase">
        <property name="createOption" value="RECREATE"/>
        <property name="operations" ref="dms.operations"/>
        <property name="templates" ref="dms.templates"/>
    </bean>
```

The createOption property above is optional and determines what SQL scripts will be run on 
service startup. Possible values are CREATE, UPDATE, and RECREATE. CREATE will attempt to
create the database schema. UPDATE will attempt to update the schema (although this is not
always possible). RECREATE will drop any existing database objects and recreate the schema
from scratch. If the option is not included, no attempt will be made to modify the schema.

Next we have some standard boilerplate for configuring the database connection:

```xml
	<bean id="datasource" class="org.springframework.jdbc.datasource.DriverManagerDataSource">
		<property name="driverClassName" value="org.h2.Driver" />
		<property name="url" value="jdbc:h2:file:/var/tmp/doctane/db" />
		<property name="username" value="sa" />
		<property name="password" value="" />
	</bean> 
```    

And configuration of the filestore:

```xml
    <bean id="filestore" class="com.softwareplumbers.dms.service.sql.LocalFilesystem">
        <property name="Path" value="/var/tmp/doctane/files"/> 
    </bean>
```

Then finally we can create the SQLRepositoryService bean itself:

```xml 
    <!--- configure base repository -->
    <bean id="base" class="com.softwareplumbers.dms.service.sql.SQLRepositoryService" scope="singleton">
        <property name="filestore" ref="filestore"/> 
    </bean>
```
 
### LocalConfig.java

The generic SQL storage service can also be configured using java annotations. However, since
the XML format allows SQL statements to be formatted more naturally and readably, we recommend
that the h2db.xml and entities.xml files should remain in XML format. As shown below, the
XML configuration for these beans can be imported into the java configuration quite simply.

```java
@ImportResource({"classpath:com/softwareplumbers/dms/service/sql/h2db.xml","classpath:com/softwareplumbers/dms/service/sql/entities.xml"})
public class LocalConfig {

    @Bean public Filestore filestore() {
        return new LocalFilesystem(Paths.get("/var/tmp/doctane/filestore"));
    }
    
    @Bean public DocumentDatabase database(
        OperationStore<DocumentDatabase.Operation> operations,
        TemplateStore<DocumentDatabase.Template> templates,
        Schema schema
    ) {
        DocumentDatabase database = new DocumentDatabase(schema);
        database.setOperations(operations);
        database.setTemplates(templates);
        return database;
    }
    
    @Bean public SQLRepositoryService service(DocumentDatabase database, Filestore filestore) throws SQLException {
        return new SQLRepositoryService(database, filestore);
    }
     
    @Bean public DataSource datasource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.h2.Driver");
        dataSourceBuilder.url("jdbc:h2:file:/var/tmp/doctane/test");
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("");
        return dataSourceBuilder.build();        
    }  
}
```