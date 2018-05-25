package uk.gov.justice.framework.tools.transformation;


import static java.util.UUID.randomUUID;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidSequenceIdException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StreamTransformationIT {

    private UUID STREAM_ID;

    private static TestEventLogJdbcRepository EVENT_LOG_JDBC_REPOSITORY;

    private static TestEventStreamJdbcRepository EVENT_STREAM_JDBC_REPOSITORY;

    private DataSource dataSource;

    private SwarmStarterUtil swarmStarterUtil = new SwarmStarterUtil();

    private LiquibaseUtil liquibaseUtil = new LiquibaseUtil();

    @Before
    public void setUpDB() throws Exception {
        STREAM_ID = randomUUID();
        dataSource = liquibaseUtil.initEventStoreDb();
        EVENT_LOG_JDBC_REPOSITORY = new TestEventLogJdbcRepository(dataSource);
        EVENT_STREAM_JDBC_REPOSITORY = new TestEventStreamJdbcRepository(dataSource);
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

    @Test
    public void shouldTransformEventInEventStore() throws Exception {
        insertEventLogData("sample.events.name", 1L);
        insertEventLogData("sample.v2.events.name", 2L);

        swarmStarterUtil.runCommand();

        assertTrue(eventStoreTransformedEventPresent());
        assertTrue(originalEventStreamIsActive());
        assertFalse(clonedStreamIsActive());
    }

    @Test
    public void shouldArchiveStreamInEventStore() throws Exception {
        insertEventLogData("sample.archive.events.name", 1L);

        swarmStarterUtil.runCommand();
        assertFalse(originalEventStreamIsActive());
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

    private boolean eventStoreTransformedEventPresent() throws SQLException {
        final Stream<Event> eventLogs = EVENT_LOG_JDBC_REPOSITORY.findAll();
        final Optional<Event> event = eventLogs.filter(item -> item.getStreamId().equals(STREAM_ID)).findFirst();

        return event.isPresent() && event.get().getName().equals("sample.events.transformedName");
    }

    private void insertEventLogData(String eventName, long sequenceId) throws SQLException, InvalidSequenceIdException {
        EVENT_LOG_JDBC_REPOSITORY.insert(eventLogFrom(eventName, sequenceId, STREAM_ID));
        EVENT_STREAM_JDBC_REPOSITORY.insert(STREAM_ID);
    }
}
