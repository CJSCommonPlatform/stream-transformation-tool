package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.fromString;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamTransformationIT {
    private static final long STREAM_COUNT_REPORTING_INTERVAL = 10L;
    private static final String MEMORY_OPTIONS_PARAMETER = "2048M";

    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;
    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;
    private UUID STREAM_ID = UUID.randomUUID();

    private SwarmStarterUtil swarmStarterUtil;

    private DatabaseUtils databaseUtils;

    private TestLinkedEventJdbcRepository linkedEventJdbcRepository;

    @Before
    public void setUp() throws Exception {
        swarmStarterUtil = new SwarmStarterUtil();
        databaseUtils = new DatabaseUtils();
        databaseUtils.dropAndUpdateLiquibase();
        linkedEventJdbcRepository = new TestLinkedEventJdbcRepository(databaseUtils.getDataSource());
    }

    @Test
    public void shouldTransformEventInEventStore() throws Exception {
        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.events.name", STREAM_ID, 1L, createdAt, 1);
        databaseUtils.insertEventLogData("sample.v2.events.name", STREAM_ID, 2L, createdAt, 2);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        final String transformedEventName = "sample.events.transformedName";
        assertThat(eventStoreTransformedEventPresent(transformedEventName), is(true));
        assertThat(originalEventStreamIsActive(), is(true));
        assertThat(clonedStreamAvailableAndActive(), is(false));
        assertThat(totalEventCount(), is(5L));

        final long linkedEventsCount = linkedEventJdbcRepository.linkedEventsCount(STREAM_ID);
        assertThat(linkedEventsCount, is(2L));

        final Stream<LinkedEvent> linkedEventStream = linkedEventJdbcRepository.findByStreamIdOrderByPositionAsc(STREAM_ID);

        linkedEventStream.forEach(linkedEvent -> {
            if (linkedEvent.getSequenceId() == 1L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is(transformedEventName));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(6L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(0L));
            }
            if (linkedEvent.getSequenceId() == 2L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is(transformedEventName));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(7L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(6L));
            }

        });
    }

    @Test
    public void shouldUseTheOriginalCreatedAtDateInTransformation() throws Exception {
        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        final String eventName = "sample.events.check-date-not-transformed";
        databaseUtils.insertEventLogData(eventName, STREAM_ID, 1L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        final List<Event> transformedEvents = getTransformedEvents();

        transformedEvents.forEach(event -> assertThat(event.getCreatedAt(), is(createdAt)));
        assertThat(totalEventCount(), is(3L));

        final long linkedEventsCount = linkedEventJdbcRepository.linkedEventsCount(STREAM_ID);
        assertThat(linkedEventsCount, is(1L));

        final Stream<LinkedEvent> linkedEventStream = linkedEventJdbcRepository
                .findByStreamIdOrderByPositionAsc(STREAM_ID);

        linkedEventStream.forEach(linkedEvent -> {

            if (linkedEvent.getSequenceId() == 1L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.events.check-date-not-transformed"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(4L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(0L));
            }
        });
    }

    @Test
    public void shouldDeactivateStreamInEventStore() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.deactivate.events.name", STREAM_ID, 1L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(originalEventStreamIsActive(), is(false));

        final long linkedEventsCount = linkedEventJdbcRepository.linkedEventsCount(STREAM_ID);
        assertThat(totalEventCount(), is(1L));

        assertThat(linkedEventsCount, is(0L));
    }

    @Test
    public void shouldPerformCustomActionOnStreamInEventStore() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.event.name.archived.old.release", STREAM_ID, 1L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(eventStoreTransformedEventPresent("sample.event.name"), is(true));
        assertThat(eventStoreEventIsPresent("sample.event.name.archived.old.release"), is(false));
        assertThat(streamAvailableAndActive(STREAM_ID), is(false));
        assertThat(clonedStreamAvailableAndActive(), is(false));
        final long linkedEventsCount = linkedEventJdbcRepository.linkedEventsCount(STREAM_ID);
        assertThat(totalEventCount(), is(1L));

        assertThat(linkedEventsCount, is(0L));

    }

    @Test
    public void shouldTransformEventByPass() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.events.name.sequence", STREAM_ID, 1L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(eventStoreTransformedEventPresent("sample.events.name.sequence2"), is(true));
        assertThat(originalEventStreamIsActive(), is(true));
        assertThat(clonedStreamAvailableAndActive(), is(false));

        assertThat(totalEventCount(), is(5L));
        final long linkedEventsCount = linkedEventJdbcRepository.linkedEventsCount(STREAM_ID);

        assertThat(linkedEventsCount, is(1L));

        final Stream<LinkedEvent> linkedEventStream = linkedEventJdbcRepository
                .findByStreamIdOrderByPositionAsc(STREAM_ID);

        linkedEventStream.forEach(linkedEvent -> {

            if (linkedEvent.getSequenceId() == 1L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.events.name.sequence2"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(7L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(0L));
            }
        });
    }

    @Test
    public void shouldTransformEventAndAddToSameStreamUsingSetStreamIdMethod() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        final String localStreamId = "80764cb1-a031-4328-b59e-6c18b0974a84";
        databaseUtils.insertEventLogData("sample.transformation.with.stream.id", fromString(localStreamId), 1L, createdAt);
        databaseUtils.insertEventLogData("sample.transformation.should.not.transform", fromString(localStreamId), 2L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(eventStoreTransformedEventPresent("sample.transformation.with.stream.id.transformed", fromString(localStreamId)), is(true));
        assertThat(eventStoreTransformedEventPresent("sample.transformation.should.not.transform", fromString(localStreamId)), is(true));
        assertThat(totalStreamCount(), is(2L));
        assertThat(totalEventCount(), is(5L));

        assertThat(clonedStreamAvailableAndActive(), is(false));

        final long linkedEventsCount = linkedEventJdbcRepository.linkedEventsCount(fromString(localStreamId));
        assertThat(linkedEventsCount, is(2L));

        final Stream<LinkedEvent> linkedEventStream = linkedEventJdbcRepository
                .findByStreamIdOrderByPositionAsc(fromString(localStreamId));

        linkedEventStream.forEach(linkedEvent -> {
            if (linkedEvent.getSequenceId() == 1L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.transformation.with.stream.id.transformed"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(6L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(0L));
            }
            if (linkedEvent.getSequenceId() == 2L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.transformation.should.not.transform"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(7L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(6L));
            }
        });
    }

    @Test
    public void shouldTransformAndMoveEventInEventStoreAndPreserveEventSequenceInTheStream() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.events.name.pass1.sequence", STREAM_ID, 1L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(totalStreamCount(), is(2L));

        final String stream2 = "80764cb1-a031-4328-b59e-6c18b0974a84";
        assertThat(eventStoreTransformedEventPresentAndSequenceCorrect("sample.events.name.pass1.sequence2", fromString(stream2), 1L), is(true));
        assertThat(eventStoreTransformedEventPresentAndSequenceCorrect("sample.events.name.pass1.sequence3", fromString(stream2), 2L), is(true));

        assertThat(eventStoreTransformedEventPresentAndSequenceCorrect("sample.events.name.pass1.sequence1", STREAM_ID ,1L), is(true));

        assertThat(totalEventCount(), is(3L));

        final long linkedEventsStream1Count = linkedEventJdbcRepository.linkedEventsCount(STREAM_ID);
        assertThat(linkedEventsStream1Count, is(1L));

        final long linkedEventsStream2Count = linkedEventJdbcRepository.linkedEventsCount(fromString(stream2));
        assertThat(linkedEventsStream2Count, is(2L));

        final Stream<LinkedEvent> linkedEventStream = linkedEventJdbcRepository
                .findByStreamIdOrderByPositionAsc(STREAM_ID);

        linkedEventStream.forEach(linkedEvent -> {
            if (linkedEvent.getSequenceId() == 1L) {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.events.name.pass1.sequence1"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(8L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(0L));
            }

        });

        final Stream<LinkedEvent> linkedEventStream2 = linkedEventJdbcRepository
                .findByStreamIdOrderByPositionAsc(fromString(stream2));
        linkedEventStream2.forEach(linkedEvent -> {
            if (linkedEvent.getSequenceId() == 1L)

            {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.events.name.pass1.sequence2"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(10L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(8L));
            }
            if (linkedEvent.getSequenceId() == 2L)

            {
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getName(), is("sample.events.name.pass1.sequence3"));
                assertThat(linkedEvent.getCreatedAt(), is(createdAt));
                assertThat(linkedEvent.getEventNumber().get(), is(11L));
                assertThat(linkedEvent.getPreviousEventNumber(), is(10L));
            }
        });
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


    private long totalEventCount() {
        return databaseUtils.getEventLogJdbcRepository().findAll().count();
    }


    private List<Event> getTransformedEvents() {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        return eventLogs.filter(item -> item.getStreamId().equals(STREAM_ID)).collect(toList());
    }
}
