# Document Management SQL Service Module

This package contains a Doctane repository service build on a generic SQL store for
metadata. Actual document files are stored using a separate service; this package
also contains an implementation of this service which uses the server's local file system
as a document store. 

This service is the _default service_ for the Doctane REST server (rest-server-core).
When build as a standalone spring boot application with the default configuration,
rest-server-core will use this service to store documents. See the rest-server-core
package [here](https://projects.softwareplumbers.com/document-management/rest-server-core)
for information about installing Doctane REST server.

## Services.xml

The services.xml file contains several spring bean definitions, both for the sql service module and for
the core Doctane server. See [Doctane Core](https://projects.softwareplumbers.com/document-management/rest-server-core) 
for information on configuring the core server. Additional configuration related to this 
service module is described below:

Firstly, we must import the database scripts needed to create and the database schema
and the SQL statements necessary to implement common Doctane operations on the database.

```xml    
    <import resource="classpath:/com/softwareplumbers/dms/service/sql/h2db.xml" />
```  

The standard h2db.xml file should be reasonably compatible with most SQL servers and
can be modified in order to support any SQL dialect. As well as the templated operations
included in the xml configuration above, the SQL service module also generates certain
statements and clauses programatically. This is done in the SQLAPIFactory class, which
is configured below:

```xml
    <bean id="SQLAPI" class="com.softwareplumbers.dms.service.sql.SQLAPIFactory" />
```

If it proves necessary to support any specific database dialect in custom code (as opposed
to changing the templates) then these changes can be implemented by creating a new version
of SQLAPIFactory.


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
    <bean id="base" class="com.softwareplumbers.dms.service.sql.SQLRepositoryService" scope="singleton">
        <property name="filestore" ref="filestore"/> 
    </bean>
```
 