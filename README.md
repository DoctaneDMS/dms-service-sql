# Document Management Filenet Service Module

This package contains the Doctane proxy for filenet. It provides a service module for the core Doctane server
which stores documents in filenet which implements the RepositoryService interface.

## Installation Instructions

The maven repository contains three key files:

* rest-server-filenet-_version_.jar
* rest-server-filenet-_version_-standalone.jar 
* rest-server-filenet-_version_-war.war

The first file is the filenet service module. It can be integrated into an existing Doctane server by
ensuring the service module jar is on the class path, and configuring the repository service bean
in services.xml. See [Doctane Core](https://projects.softwareplumbers.com/document-management/rest-server-core)
for more information.

The second file is a standalone spring boot application, integrating the filenet service module with
a standard build of rest-server-core, that can be started directly from the command line:

`java -jar rest-server-filenet-master-standalone.jar`

The third file is a standard WAR file that can be deployed on any JEE application server (deployment is tested on Tomcat 9)

In either case, some configuration is required. The a default configuration file services.xml is built into the jar/war file. This configuration file can be edited by opening the jar file in a utility such as winzip, or overridden by placing a custom configuration directory on the classpath in such a way that it will override the built-in file.

## Services.xml

The services.xml file contains several spring bean definitions, both for the filenet service module and for
the core Doctane server. See [Doctane Core](https://projects.softwareplumbers.com/document-management/rest-server-core) 
for information on configuring the core server. Additional filenet-related configuration is described below:

The first two bean definitions point to additional configuration files which must also be on the classpath. These 'schema' 
files control what data can be stored for a document or workspace, and are described in more detail in the following section. 
The ids of these beans are not important in themeselves, they are simply referenced later in the file.

```xml
<bean id="documentSchema" class="com.softwareplumbers.dms.rest.server.filenet.Schema" scope="singleton">
	<constructor-arg value="Schema.json"/>
</bean>

<bean id="workspaceSchema" class="com.softwareplumbers.dms.rest.server.filenet.Schema" scope="singleton">
	<constructor-arg value="WorkspaceSchema.json"/>
</bean>
```

The following bean configures the actual filenet repository service. This must be mapped to one or more tenants
in the core RepositoryServiceFactory bean.

```xml
   <bean id="test" class="com.softwareplumbers.dms.rest.server.filenet.FilenetRepositoryService" scope="singleton">
     <constructor-arg index="0" ref="workspaceSchema"/>
     <constructor-arg index="1" ref="documentSchema"/>
     <constructor-arg index="2" value="http://filenet1.directory.softwareplumbers.net:9080/wsi/FNCEWS40MTOM"/>
     <constructor-arg index="3" value="testuser"/>
     <constructor-arg index="4" value="carrot22"/>
     <constructor-arg index="5" value="P8Domain"/>
     <constructor-arg index="6" value="DemoObjectStore"/>
     <constructor-arg index="7" value="qasys"/>
   </bean>
```

* Arguments 0 and 1 refer to the workspace and document schema beans defined earlier in the file.
* Argument 2 is the URI of the filenet web service
* Arguments 3 and 4 provide the username and password for connecting to the filenet service
* Arguments 5 and 6 define the Filenet Domain and Object Store used to store documents and metadata
* Argument 7 defines a base folder that all Doctane objects will be stored inside.


## Schema Files

Both schema files have a similar JSON format:

```json
{ 
	"base"	:		"DoctaneBase",
    "class"	: 		"DocumentTrade",
  	"properties": {
  			"DocumentTitle": { "type": "STRING" },
  			"DocCategory": 	{ "type":"STRING" },
  			"BatchID": 		{ "type":"STRING" },
  			"Customer": 	{ "type":"STRING" },
  			"TradeDescription": 	{ "type":"STRING" },
  			"DocFaceRef": 	{ "type":"STRING" },
  			"DocType": 		{ "type":"STRING" },
  			"Event": 		{ "type":"STRING" },
  			"EventDescription": 	{ "type":"STRING" },
  			"Product": 		{ "type":"STRING" },
  			"ProductDescription":	{ "type":"STRING" },
  			"Branch": 		{ "type":"STRING" },
  			"Team": 		{ "type":"STRING" },
  			"OurReference": { "type":"STRING" },
  			"TheirReference": { "type":"STRING" },
  			"BankDocument": { "type":"STRING" },
  			"FinalForm": 	{ "type":"STRING" },
  			"Zone": 		{ "type":"STRING" },
  			"SourceBankingBusiness": { "type":"STRING"}
  	}
}   
```

* `base` should be either `DoctaneBase` (for a document) or `DoctaneWorkspace` (for a workspace)
* `class` refers to the name of the Filenet object type, which must be set up in Filenet
* `properties` enumerates all the valid properties of the given type. In each case the given name must exactly match a property name which is valid for the given class in FileNet. The parameter `type` may be one of STRING, DATETIME, INTEGER, BOOLEAN, DOUBLE, ID, OBJECT.

If a client attempts to write a property which is not defined in one of the schemas, the doctane server will ignore it. If, on the other hand, a property is specified in the schema which is not valid for the filenet object class, then an error will occur when any attempt is made to write that property.
 