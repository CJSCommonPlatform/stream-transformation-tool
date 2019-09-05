package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

public class StreamTransformationMoveIT {

    private static final long STREAM_COUNT_REPORTING_INTERVAL = 10L;
    private static final String MEMORY_OPTIONS_PARAMETER = "2048M";
    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;
    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;
    private static final UUID STREAM_ID = randomUUID();

    private SwarmStarterUtil swarmStarterUtil;
    private DatabaseUtils databaseUtils;
    private TestPublishedEventJdbcRepository testPublishedEventJdbcRepository;

    @Before
    public void setUp() throws Exception {
        swarmStarterUtil = new SwarmStarterUtil();
        databaseUtils = new DatabaseUtils();
        databaseUtils.dropAndUpdateLiquibase();
        testPublishedEventJdbcRepository = new TestPublishedEventJdbcRepository(databaseUtils.getDataSource());
    }

    @Test
    public void shouldMoveEventInEventStore() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.transformation.move.1", STREAM_ID, 1L, createdAt);
        databaseUtils.insertEventLogData("sample.events.name.should.not.be.transformed", STREAM_ID, 2L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(totalStreamCount(), is(5L));
        assertThat(totalClonedStreamsCreated(), is(3L));

        final String transformedEventName = "sample.transformation.move.4";
        assertTrue(eventNameExist(transformedEventName));

        final long publishedEventsCount = testPublishedEventJdbcRepository.publishedEventsCount(STREAM_ID);
        assertThat(publishedEventsCount, is(1L));

        final List<PublishedEvent> publishedEvents = testPublishedEventJdbcRepository.findAllOrderByPositionAsc();

        assertThat(publishedEvents.size(), is(2));

        final PublishedEvent publishedEvent_1 = publishedEvents.get(0);
        assertThat(publishedEvent_1.getName(), is("sample.events.name.should.not.be.transformed"));
        assertThat(publishedEvent_1.getCreatedAt(), is(createdAt));
        assertThat(publishedEvent_1.getEventNumber(), is(Optional.of(3L)));
        assertThat(publishedEvent_1.getPreviousEventNumber(), is(0L));

        final PublishedEvent publishedEvent_2 = publishedEvents.get(1);
        assertThat(publishedEvent_2.getName(), is("sample.transformation.move.4"));
        assertThat(publishedEvent_2.getCreatedAt(), is(createdAt));
        assertThat(publishedEvent_2.getEventNumber(), is(Optional.of(5L)));
        assertThat(publishedEvent_2.getPreviousEventNumber(), is(3L));

        assertThat(testPublishedEventJdbcRepository.prePublishQueueCount(), is(0L));
    }

    @Test
    public void shouldMoveEventInEventStoreWithoutBackup() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.transformation.move.without.backup", STREAM_ID, 1L, createdAt);
        databaseUtils.insertEventLogData("sample.events.name.passer1", STREAM_ID, 2L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        assertThat(totalStreamCount(), is(2L));
        assertThat(totalClonedStreamsCreated(), is(0L));
        assertTrue(eventNameExist("sample.transformation.move.without.backup.transformed"));

        final long publishedEventsCount = testPublishedEventJdbcRepository.publishedEventsCount(STREAM_ID);
        assertThat(publishedEventsCount, is(1L));

        final List<PublishedEvent> publishedEvents = testPublishedEventJdbcRepository.findAllOrderByPositionAsc();

        assertThat(publishedEvents.size(), is(2));

        final PublishedEvent publishedEvent_1 = publishedEvents.get(0);
        assertThat(publishedEvent_1.getName(), is("sample.transformation.move.without.backup.transformed"));
        assertThat(publishedEvent_1.getCreatedAt(), is(createdAt));
        assertThat(publishedEvent_1.getEventNumber(), is(Optional.of(1L)));
        assertThat(publishedEvent_1.getPreviousEventNumber(), is(0L));

        final PublishedEvent publishedEvent_2 = publishedEvents.get(1);
        assertThat(publishedEvent_2.getName(), is("sample.events.name.passer1"));
        assertThat(publishedEvent_2.getCreatedAt(), is(createdAt));
        assertThat(publishedEvent_2.getEventNumber(), is(Optional.of(2L)));
        assertThat(publishedEvent_2.getPreviousEventNumber(), is(1L));

        assertThat(testPublishedEventJdbcRepository.prePublishQueueCount(), is(0L));
    }

    private boolean eventNameExist(final String eventName) {
        final Optional<Event> eventOptional = databaseUtils.getEventStoreDataAccess()
                .findAllEvents().stream()
                .filter(event -> event.getName().equals(eventName))
                .findFirst();

        return eventOptional.isPresent();
    }

    private long totalStreamCount() {
        return databaseUtils.getEventStreamJdbcRepository().findAll().size();
    }

    private long totalClonedStreamsCreated() {
        final Stream<Event> eventStream = databaseUtils.getEventStoreDataAccess()
                .findAllEvents().stream()
                .filter(event -> event.getName().equals("system.events.cloned"));

        return eventStream.count();
    }
}
