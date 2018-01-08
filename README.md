# stream-transformation-tool

[![Build Status](https://travis-ci.org/CJSCommonPlatform/stream-transformation-tool.svg?branch=master)](https://travis-ci.org/CJSCommonPlatform/stream-transformation-tool) [![Coverage Status](https://coveralls.io/repos/github/CJSCommonPlatform/stream-transformation-tool/badge.svg?branch=master)](https://coveralls.io/github/CJSCommonPlatform/stream-transformation-tool?branch=master)

The stream-transformation-tool is a tool to support transformations of the event_store implemented within the [microservice_framework](https://github.com/CJSCommonPlatform/microservice_framework)

> Note: The event-tool is a WildFly Swarm application.

> Note: The event-tool requires 'Event Transformations' classes to be loaded at runtime to provide the actual transformation instructions

# Modules

* event-tool - The WildFly Swarm event-transformation application
* stream-transformation-test - Test module, with sample Event Transformations and integration test
* stream-transformation-tool-api - API exposing the externally facing components of the repository
* stream-transformation-fraction - Swarm fraction for bootstrapping the Swarm Application
* stream-transformation-service - The internal implementation of the transformation process

# How to Build the Event-Tool

The stream-transformation-tool attempts to be a well-behaved Maven project. To install to your local repository for usage:
```bash
mvn clean install
```

The WildFly Swarm application `event-tool-x.x.x-swarm.jar` will be produced in the `event-tool/target/` directory.

# Running the Event-Tool

To run the event-tool WildFly Swarm application you require:
* The built application jar: `event-tool-0.0.1-SNAPSHOT-swarm.jar`
* A configuration file with datasource of the event_store to be transformed: `standalone-ds.xml`
* A jar containing your Event Transformations: `stream-transformations-0.0.1-SNAPSHOT.jar` 

Additionally to trigger the automatic shutdown hook on completion you need to create a file and provide the fully qualified file as `org.wildfly.swarm.mainProcessFile` System property

```bash
touch processFile
java -jar -Dorg.wildfly.swarm.mainProcessFile=/Fully/Qualified/Path/processFile -Devent.transformation.jar=stream-transformations-0.0.1-SNAPSHOT.jar event-tool-0.0.1-SNAPSHOT-swarm.jar -c standalone-ds.xml
```

## ManagedExecutorService Configuration

To customise the `ManagedExecutorService` (threads etc.) you can alter [StartTransformation.java](https://github.com/CJSCommonPlatform/stream-transformation-tool/blob/master/event-tool/src/main/java/uk/gov/justice/event/tool/StartTransformation.java) to add a named Resource

```java
@Resource(name="event-tool")
private ManagedExecutorService executorService;
```

This will require additional configuration in your `standalone-ds.xml`

```xml
<subsystem xmlns="urn:jboss:domain:ee:4.0">
    <concurrent>
    ...
        <managed-executor-services>
            <managed-executor-service
                            name="event-tool"
                            jndi-name="java:jboss/ee/concurrency/executor/default"
                            context-service="default"
                            thread-factory="default"
                            hung-task-threshold="60000"
                            core-threads="1"
                            max-threads="5"
                            keepalive-time="5000"
                            queue-length="1000000"
                            reject-policy="RETRY_ABORT" />
        </managed-executor-services>
    </concurrent>
    <default-bindings
        context-service="java:jboss/ee/concurrency/context/default"
        jms-connection-factory="java:jboss/DefaultJMSConnectionFactory"
        managed-executor-service="java:jboss/ee/concurrency/executor/default"
        managed-thread-factory="java:jboss/ee/concurrency/factory/default" />
</subsystem>
```

# How to Build the accompanying EventTransformation jar

The event tool application scans on start-up beans annotated with [@Transformation](https://github.com/CJSCommonPlatform/stream-transformation-tool/blob/master/stream-transformation-tool-api/src/main/java/uk/gov/justice/tools/eventsourcing/transformation/api/annotation/Transformation.java), which implement the [EventTransformation](https://github.com/CJSCommonPlatform/stream-transformation-tool/blob/master/stream-transformation-tool-api/src/main/java/uk/gov/justice/tools/eventsourcing/transformation/api/EventTransformation.java) interface.

Create a jar file that depends upon the `stream-transformation-tool-api` module that satisfies the above, with the implementation details specifying when to apply the transformation and how to transform the event.

For an example of this please see `stream-transformation-tool-test/sample-transformations`
