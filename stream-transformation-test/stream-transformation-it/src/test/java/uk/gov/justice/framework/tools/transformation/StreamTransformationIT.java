package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.fromString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamTransformationIT {

    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;
    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;

    private UUID STREAM_ID = UUID.randomUUID();

    private SwarmStarterUtil swarmStarterUtil = new SwarmStarterUtil();

    private DatabaseUtils databaseUtils;

    @Before
    public void setUp() throws Exception {
        databaseUtils = new DatabaseUtils();
    }

    @After
    public void cleanup() throws Exception {
        databaseUtils.resetDatabase();
    }


    @Test
    public void shouldTransformEventInEventStore() throws Exception {
        databaseUtils.insertEventLogData("sample.events.name", STREAM_ID, 1L);
        databaseUtils.insertEventLogData("sample.v2.events.name", STREAM_ID, 2L);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(eventStoreTransformedEventPresent("sample.events.transformedName"), is(true));
        assertThat(originalEventStreamIsActive(), is(true));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }

    @Test
    public void shouldDeactivateStreamInEventStore() throws Exception {
        databaseUtils.insertEventLogData("sample.deactivate.events.name", STREAM_ID, 1L);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(originalEventStreamIsActive(), is(false));
    }

    @Test
    public void shouldPerformCustomActionOnStreamInEventStore() throws Exception {
        databaseUtils.insertEventLogData("sample.event.name.archived.old.release", STREAM_ID, 1L);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(eventStoreTransformedEventPresent("sample.event.name"), is(true));
        assertThat(eventStoreEventIsPresent("sample.event.name.archived.old.release"), is(false));
        assertThat(streamAvailableAndActive(STREAM_ID), is(false));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }

    @Test
    public void shouldTransformEventByPass() throws Exception {
        databaseUtils.insertEventLogData("sample.events.name.sequence", STREAM_ID, 1L);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(eventStoreTransformedEventPresent("sample.events.name.sequence2"), is(true));
        assertThat(originalEventStreamIsActive(), is(true));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }


    @Test
    public void shouldTransformEventAndAddToSameStreamUsingSetStreamIdMethod() throws Exception {
        databaseUtils.insertEventLogData("sample.transformation.with.stream.id", fromString("80764cb1-a031-4328-b59e-6c18b0974a84"), 1L);
        databaseUtils.insertEventLogData("sample.transformation.should.not.transform", fromString("80764cb1-a031-4328-b59e-6c18b0974a84"), 2L);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(eventStoreTransformedEventPresent("sample.transformation.with.stream.id.transformed", fromString("80764cb1-a031-4328-b59e-6c18b0974a84")), is(true));
        assertThat(eventStoreTransformedEventPresent("sample.transformation.should.not.transform", fromString("80764cb1-a031-4328-b59e-6c18b0974a84")), is(true));
        assertThat(totalStreamCount(), is(2L));
        assertThat(clonedStreamAvailableAndActive(), is(false));
    }

    @Test
    public void shouldTransformAndMoveEventInEventStoreAndPreserveEventSequenceInTheStream() throws Exception {
        databaseUtils.insertEventLogData("sample.events.name.pass1.sequence", STREAM_ID, 1L);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(totalStreamCount(), is(2L));

        assertThat(eventStoreTransformedEventPresentAndSequenceCorrect("sample.events.name.pass1.sequence2", fromString("80764cb1-a031-4328-b59e-6c18b0974a84") ,1L), is(true));
        assertThat(eventStoreTransformedEventPresentAndSequenceCorrect("sample.events.name.pass1.sequence3", fromString("80764cb1-a031-4328-b59e-6c18b0974a84"), 2L), is(true));

        assertThat(eventStoreTransformedEventPresentAndSequenceCorrect("sample.events.name.pass1.sequence1", STREAM_ID ,1L), is(true));
    }


    private boolean clonedStreamAvailableAndActive() {
        final Optional<Event> matchingClonedEvent = databaseUtils.getEventLogJdbcRepository().findAll()
                .filter(event -> event.getName().equals("system.events.cloned"))
                .findFirst();
        return matchingClonedEvent.isPresent()
                && streamAvailableAndActive(matchingClonedEvent.get().getStreamId());
    }

    private boolean originalEventStreamIsActive() {
        return streamAvailableAndActive(STREAM_ID);
    }

    private boolean streamAvailableAndActive(final UUID streamId) {
        final Optional<EventStream> matchingEvent = databaseUtils.getEventStreamJdbcRepository().findAll()
                .filter(eventStream -> eventStream.getStreamId().equals(streamId))
                .findFirst();
        return matchingEvent.isPresent() && matchingEvent.get().isActive();
    }

    private boolean eventStoreTransformedEventPresent(final String transformedEventName) {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        final Optional<Event> event = eventLogs.filter(item -> item.getStreamId().equals(STREAM_ID)).findFirst();

        return event.isPresent() && event.get().getName().equals(transformedEventName);
    }

    private boolean eventStoreTransformedEventPresentAndSequenceCorrect(final String transformedEventName, final UUID streamId, final long sequence) {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        final Optional<Event> event = eventLogs
                .filter(item -> item.getStreamId().equals(streamId))
                .filter(item -> item.getName().equals(transformedEventName))
                .findFirst();

        return event.isPresent() && event.get().getSequenceId().equals(sequence);
    }

    private boolean eventStoreTransformedEventPresent(final String transformedEventName, final UUID streamId) {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        final Optional<Event> event = eventLogs
                .filter(item -> item.getStreamId().equals(streamId))
                .filter(item -> item.getName().equals(transformedEventName))
                .findFirst();

        return event.isPresent() ;
    }

    private boolean eventStoreEventIsPresent(final String originalEventName) {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        final Optional<Event> event = eventLogs
                .filter(item -> item.getName().equals(originalEventName))
                .filter(item -> item.getStreamId().equals(STREAM_ID))
                .findFirst();

        return event.isPresent();
    }

    private long totalStreamCount() {
        return databaseUtils.getEventStreamJdbcRepository().findAll().count();
    }

}
