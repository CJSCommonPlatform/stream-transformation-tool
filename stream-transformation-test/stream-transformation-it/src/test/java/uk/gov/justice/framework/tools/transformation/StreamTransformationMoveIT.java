package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamTransformationMoveIT {

    private static final UUID STREAM_ID = randomUUID();

    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;

    private static final long STREAMS_PROCESSED_COUNT_STEP_INFO = 100;

    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;

    private static final String MEMORY_OPTIONS = "2048Mb";

    private SwarmStarterUtil swarmStarterUtil;

    private DatabaseUtils databaseUtils;

    @Before
    public void setUp() throws Exception {
        swarmStarterUtil = new SwarmStarterUtil();
        databaseUtils = new DatabaseUtils();
        databaseUtils.dropAndUpdateLiquibase();
    }

    @After
    public void cleanup() throws Exception {
        databaseUtils.resetDatabase();
    }

    @Test
    public void shouldMoveEventInEventStore() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.transformation.move.1", STREAM_ID, 1L, createdAt);
        databaseUtils.insertEventLogData("sample.events.name.should.not.be.transformed", STREAM_ID, 2L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(totalStreamCount(), is(5L));
        assertThat(totalClonedStreamsCreated(), is(3L));
        assertTrue(eventNameExist("sample.transformation.move.4"));
    }

    @Test
    public void shouldMoveEventInEventStoreWithoutBackup() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData("sample.transformation.move.without.backup", STREAM_ID, 1L, createdAt);
        databaseUtils.insertEventLogData("sample.events.name.passer1", STREAM_ID, 2L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        assertThat(totalStreamCount(), is(2L));
        assertThat(totalClonedStreamsCreated(), is(0L));
        assertTrue(eventNameExist("sample.transformation.move.without.backup.transformed"));
    }

    private boolean eventNameExist(final String eventName) {
        final Optional<Event> eventOptional = databaseUtils.getEventLogJdbcRepository()
                .findAll()
                .filter(event -> event.getName().equals(eventName))
                .findFirst();

        return eventOptional.isPresent();
    }

    private long totalStreamCount() {
        return databaseUtils.getEventStreamJdbcRepository().findAll().count();
    }

    private long totalClonedStreamsCreated() {
        final Stream<Event> eventStream = databaseUtils.getEventLogJdbcRepository()
                .findAll()
                .filter(event -> event.getName().equals("system.events.cloned"));

        return eventStream.count();
    }
}
