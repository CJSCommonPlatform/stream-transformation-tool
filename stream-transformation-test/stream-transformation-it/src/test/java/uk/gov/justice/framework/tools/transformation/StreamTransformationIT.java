package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;
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

    private static TestEventLogJdbcRepository EVENT_LOG_JDBC_REPOSITORY;
    private static TestEventStreamJdbcRepository EVENT_STREAM_JDBC_REPOSITORY;

    private UUID STREAM_ID;

    private SwarmStarterUtil swarmStarterUtil = new SwarmStarterUtil();

    private LiquibaseUtil liquibaseUtil = new LiquibaseUtil();

    @Before
    public void setUpDB() throws Exception {
        STREAM_ID = randomUUID();
        final DataSource dataSource = liquibaseUtil.initEventStoreDb();
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

        assertThat(eventStoreTransformedEventPresent("sample.events.transformedName"), is(true));
        assertThat(originalEventStreamIsActive(), is(true));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }

    @Test
    public void shouldDeactivateStreamInEventStore() throws Exception {
        insertEventLogData("sample.deactivate.events.name", 1L);

        swarmStarterUtil.runCommand();

        assertThat(originalEventStreamIsActive(), is(false));
    }

    @Test
    public void shouldPerformCustomActionOnStreamInEventStore() throws Exception {
        insertEventLogData("sample.event.name.archived.old.release", 1L);

        swarmStarterUtil.runCommand();

        assertThat(eventStoreTransformedEventPresent("sample.event.name"), is(true));
        assertThat(eventStoreOriginalEventIsPresent("sample.event.name.archived.old.release"), is(false));
        assertThat(streamAvailableAndActive(STREAM_ID), is(false));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }

    @Test
    public void shouldTransformEventByPass() throws Exception {
        insertEventLogData("sample.events.name.pass", 1L);

        swarmStarterUtil.runCommand();

        assertThat(eventStoreTransformedEventPresent("sample.events.transformedName.pass2"), is(true));
        assertThat(originalEventStreamIsActive(), is(true));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }

    private boolean clonedStreamAvailableAndActive() {
        final Optional<Event> matchingClonedEvent = EVENT_LOG_JDBC_REPOSITORY.findAll()
                .filter(event -> event.getName().equals("system.events.cloned"))
                .findFirst();
        return matchingClonedEvent.isPresent() 
                && streamAvailableAndActive(matchingClonedEvent.get().getStreamId());
    }

    private boolean originalEventStreamIsActive() {
        return streamAvailableAndActive(STREAM_ID);
    }

    private boolean streamAvailableAndActive(final UUID streamId) {
        final Optional<EventStream> matchingEvent = EVENT_STREAM_JDBC_REPOSITORY.findAll()
                .filter(eventStream -> eventStream.getStreamId().equals(streamId))
                .findFirst();
        return matchingEvent.isPresent() && matchingEvent.get().isActive();
    }

    private boolean eventStoreTransformedEventPresent(final String transformedEventName) {
        final Stream<Event> eventLogs = EVENT_LOG_JDBC_REPOSITORY.findAll();
        final Optional<Event> event = eventLogs.filter(item -> item.getStreamId().equals(STREAM_ID)).findFirst();

        return event.isPresent() && event.get().getName().equals(transformedEventName);
    }

    private boolean eventStoreOriginalEventIsPresent(final String originalEventName) {
        final Stream<Event> eventLogs = EVENT_LOG_JDBC_REPOSITORY.findAll();
        final Optional<Event> event = eventLogs.filter(item -> item.getName().equals(originalEventName)).findFirst();

        return event.isPresent();
    }

    private void insertEventLogData(String eventName, long sequenceId) throws InvalidSequenceIdException {
        EVENT_LOG_JDBC_REPOSITORY.insert(eventLogFrom(eventName, sequenceId, STREAM_ID));
        EVENT_STREAM_JDBC_REPOSITORY.insert(STREAM_ID);
    }
}
