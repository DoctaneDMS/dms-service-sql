# Document Management SQL Service Module

This package contains a Doctane repository service build on a generic SQL store for
metadata. Actual document files are stored using a separate service; this package
also contains an implementation of this service which uses the server's local file system
as a document store. 

This service is the _default service_ for the Doctane REST server for documents (rest-server-dms).
When build as a standalone spring boot application with the default configuration,
rest-server-dms will use this service to store documents. See the rest-server-dms
package [here](https://projects.softwareplumbers.com/document-management/rest-server-dms)
for information about installing the Doctane REST server for documents.

## Configuration

### Environment variables

In many cases is module can be configured by simply setting a few environment variables:

* DOCTANE_INSTALLATION_ROOT - local directory to which Doctane can write files
* DOCTANE_DATABASE_URL - the jdbc URL of the database to be used
* DOCTANE_DATABASE_USER - the username to use
* DOCTANE_DATABASE_PASSWORD - the password to use
* DOCTANE_DATABASE_CREATE_OPTION - (NONE, CREATE, UPDATE, REPLACE) - controls creation/update 
of the database tables and views on application startup. Recommend 'NONE' in production

For more advanced configurations it will be necessary to edit the services.xml file as
described below.

### services.xml

The services.xml file contains several spring bean definitions for the sql service module. 
Additional configuration related to this service module is described below:

Firstly, we must import the database scripts needed to create and the database schema
and the SQL statements necessary to implement common Doctane operations on the database.

```xml    
    <import resource="classpath:/com/softwareplumbers/dms/service/sql/h2db.xml" />
    <import resource="classpath:/com/softwareplumbers/dms/service/sql/mysqldb.xml" />
    <import resource="classpath:/com/softwareplumbers/dms/service/sql/entities.xml" />
```  

Each supported database type has an xml file which contains the SQL statements needed
to create and operate a document database. To detect which database type you are using
and apply the correct configuration, you must create a DatabaseConfigFactory bean like
the below.

```xml
    <bean id="DmsDatabaseConfigFactory"
            class="org.springframework.beans.factory.config.ServiceLocatorFactoryBean">
        <property name="serviceLocatorInterface" value="com.softwareplumbers.common.sql.DatabaseConfigFactory"/>
        <property name="serviceMappings">
            <props>
                <prop key="MYSQL">mysql.dms.config</prop>
                <prop key="H2">h2.dms.config</prop>
            </props>
        </property>
    </bean>   
```xml    

As well as the templated operations included in the xml configuration above, the SQL service
module also generates certain statements and clauses programatically. This is done in the 
DocumentDatabase class, which is configured below:

```xml   
    <bean id="database" class="com.softwareplumbers.dms.service.sql.DocumentDatabase" scope="singleton">
        <constructor-arg index="0" value="#{systemEnvironment['DOCTANE_DATABASE_URL']}"/>
        <constructor-arg index="1">
            <props>
                <prop key="username">#{@K8SEnvironment.secrets['SCHEMA_USERNAME']?:systemEnvironment['DOCTANE_DATABASE_USER']}</prop>
                <prop key="password">#{@K8SEnvironment.secrets['SCHEMA_PASSWORD']?:systemEnvironment['DOCTANE_DATABASE_PASSWORD']}</prop>
            </props>
        </constructor-arg>
        <constructor-arg index="2" ref="DmsDatabaseConfigFactory"/>                
        <constructor-arg index="3" value="#{systemEnvironment['DOCTANE_DATABASE_CREATE_OPTION']}"/>
    </bean>
```

Additional properties which may be of user are driverClassName (to point the system at a given driver - not always necessary
if the driver resides directly on the classpath) and connectionInitSql (which can be used to set the default schema if
the specified user is not the schema owner).

The createOption property above is optional and determines what SQL scripts will be run on 
service startup. Possible values are NONE, CREATE, UPDATE, and RECREATE. CREATE will attempt to
create the database schema. UPDATE will attempt to update the schema (although this is not
always possible). RECREATE will drop any existing database objects and recreate the schema
from scratch. If the option is not included, no attempt will be made to modify the schema.

Next we have some standard boilerplate for configuring the the filestore:

```xml
    <bean id="filestore" class="com.softwareplumbers.dms.service.sql.LocalFilesystem">
        <property name="PathParts">
            <array>
                <value>#{systemEnvironment['DOCTANE_INSTALLATION_ROOT']}</value>
                <value>documents</value>
            </array>
        </property>
    </bean>
```

Then finally we can create the SQLRepositoryService bean itself:

```xml 
    <bean id="base" class="com.softwareplumbers.dms.service.sql.SQLRepositoryService" scope="singleton">
        <constructor-arg ref="dms.database"/>
        <constructor-arg ref="filestore"/> 
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
}
```