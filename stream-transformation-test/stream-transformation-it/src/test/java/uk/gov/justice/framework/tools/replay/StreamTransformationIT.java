package uk.gov.justice.framework.tools.replay;


import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.services.test.utils.core.messaging.JsonEnvelopeBuilder.envelope;
import static uk.gov.justice.services.test.utils.core.messaging.MetadataBuilderFactory.metadataWithRandomUUID;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidSequenceIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import javax.sql.DataSource;

import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StreamTransformationIT {

    private static final TestProperties TEST_PROPERTIES = new TestProperties("test.properties");

    private static final UUID STREAM_ID = randomUUID();

    private static TestEventLogJdbcRepository EVENT_LOG_JDBC_REPOSITORY;

    private static TestEventStreamJdbcRepository EVENT_STREAM_JDBC_REPOSITORY;

    private DataSource dataSource;

    @Before
    public void setUpDB() throws Exception {
        dataSource = initEventStoreDb();
        EVENT_LOG_JDBC_REPOSITORY = new TestEventLogJdbcRepository(dataSource);
        EVENT_STREAM_JDBC_REPOSITORY = new TestEventStreamJdbcRepository(dataSource);
    }

    @Test
    public void shouldTransformEventInEventStore() throws Exception {
        insertEventLogData();

        runCommand(createCommandToExecuteTransformationTool());

        assertTrue(eventStoreTransformedEventPresent());
        assertTrue(originalEventStreamIsActive());
        assertFalse(clonedStreamIsActive());
    }

    private boolean clonedStreamIsActive() {
        final UUID clonedStreamId = EVENT_LOG_JDBC_REPOSITORY.findAll()
                .filter(event -> event.getName().equals("system.events.cloned"))
                .findFirst().get()
                .getStreamId();

        final boolean isActive = getStreamIsActiveStreamById(clonedStreamId);

        return isActive;
    }

    private boolean originalEventStreamIsActive() {
        final boolean isActive = getStreamIsActiveStreamById(STREAM_ID);
        return isActive;
    }

    private boolean getStreamIsActiveStreamById(UUID streamId) {
        final boolean isActive = EVENT_STREAM_JDBC_REPOSITORY.findAll()
                .filter(eventStream -> eventStream.getStreamId().equals(streamId))
                .findFirst().get().isActive();

        return isActive;
    }

    @After
    public void tearDown() throws SQLException {
        final PreparedStatement preparedStatement = EVENT_LOG_JDBC_REPOSITORY.getDataSource().getConnection().prepareStatement("delete from event_log");
        preparedStatement.executeUpdate();
        EVENT_LOG_JDBC_REPOSITORY.getDataSource().getConnection().close();

        final PreparedStatement preparedStatement1 = EVENT_LOG_JDBC_REPOSITORY.getDataSource().getConnection().prepareStatement("delete from event_stream");
        preparedStatement1.executeUpdate();
        EVENT_STREAM_JDBC_REPOSITORY.getDatasource().getConnection().close();
    }

    private static DataSource initEventStoreDb() throws Exception {
        return initDatabase("db.eventstore.url",
                "db.eventstore.userName",
                "db.eventstore.password",
                "liquibase/event-store-db-changelog.xml");
    }

    private Event eventLogFrom(final String eventName, final Long sequenceId) {
        final JsonEnvelope jsonEnvelope = envelope()
                .with(metadataWithRandomUUID(eventName)
                        .createdAt(ZonedDateTime.now())
                        .withVersion(sequenceId)
                        .withStreamId(STREAM_ID))
                .withPayloadOf("test", "a string")
                .build();

        final Metadata metadata = jsonEnvelope.metadata();
        final UUID id = metadata.id();

        final UUID streamId = metadata.streamId().get();
        final String name = metadata.name();
        final String payload = jsonEnvelope.payloadAsJsonObject().toString();
        final ZonedDateTime createdAt = metadata.createdAt().get();

        return new Event(id, streamId, sequenceId, name, metadata.asJsonObject().toString(), payload, createdAt);
    }

    private String createCommandToExecuteTransformationTool() throws IOException {
        final String eventToolJarLocation = getResource("event-tool*.jar");
        final String streamJarLocation = getResource("stream-transformations*.jar");
        final String standaloneDSLocation = getResource("standalone-ds.xml");
        final String mainProcessFilePath = Paths.get(File.createTempFile("mainProcessFile", "tmp").toURI()).toAbsolutePath().toString();

        final String command = commandFrom(mainProcessFilePath, streamJarLocation, eventToolJarLocation, standaloneDSLocation);

        return command;
    }

    private String commandFrom(final String mainProcessFilePath,
                               final String streamJarLocation,
                               final String eventToolJarLocation,
                               final String standaloneDSLocation) {
        return format("java -jar -Dorg.wildfly.swarm.mainProcessFile=%s -Devent.transformation.jar=%s %s -c %s",
                mainProcessFilePath,
                streamJarLocation,
                eventToolJarLocation,
                standaloneDSLocation);
    }

    private static DataSource initDatabase(final String dbUrlPropertyName,
                                           final String dbUserNamePropertyName,
                                           final String dbPasswordPropertyName,
                                           final String... liquibaseChangeLogXmls) throws Exception {
        final BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriverClassName(TEST_PROPERTIES.value("db.driver"));

        dataSource.setUrl(TEST_PROPERTIES.value(dbUrlPropertyName));
        dataSource.setUsername(TEST_PROPERTIES.value(dbUserNamePropertyName));
        dataSource.setPassword(TEST_PROPERTIES.value(dbPasswordPropertyName));
        boolean dropped = false;
        final JdbcConnection jdbcConnection = new JdbcConnection(dataSource.getConnection());

        for (String liquibaseChangeLogXml : liquibaseChangeLogXmls) {
            Liquibase liquibase = new Liquibase(liquibaseChangeLogXml,
                    new ClassLoaderResourceAccessor(), jdbcConnection);
            if (!dropped) {
                liquibase.dropAll();
                dropped = true;
            }
            liquibase.update("");
        }
        return dataSource;
    }

    private boolean eventStoreTransformedEventPresent() throws SQLException {
        final Stream<Event> eventLogs = EVENT_LOG_JDBC_REPOSITORY.findAll();
        final Optional<Event> event = eventLogs.filter(item -> item.getStreamId().equals(STREAM_ID)).findFirst();

        return event.isPresent() && event.get().getName().equals("sample.events.transformedName");
    }

    private String getResource(final String pattern) {
        final File dir = new File(this.getClass().getClassLoader().getResource("").getPath());
        final FileFilter fileFilter = new WildcardFileFilter(pattern);
        return dir.listFiles(fileFilter)[0].getAbsolutePath();
    }

    public void runCommand(final String command) throws Exception {
        final Process exec = Runtime.getRuntime().exec(command);
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(exec.getInputStream()));

        String line = "";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    private void insertEventLogData() throws SQLException, InvalidSequenceIdException {
        EVENT_LOG_JDBC_REPOSITORY.insert(eventLogFrom("sample.events.name", 1L));
        EVENT_STREAM_JDBC_REPOSITORY.insert(STREAM_ID);
    }

    private void insertPerformanceTestData() throws SQLException, InvalidSequenceIdException {
        List<UUID> streamIds = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            streamIds.add(randomUUID());
        }

        for (UUID id : streamIds) {
            for (int i = 1; i < 1001; i++) {
                Long logId = new Long(i);
                EVENT_LOG_JDBC_REPOSITORY.insert(eventLogFrom("sample.events.name", logId));

            }
            EVENT_STREAM_JDBC_REPOSITORY.insert(id);

        }

    }
}
