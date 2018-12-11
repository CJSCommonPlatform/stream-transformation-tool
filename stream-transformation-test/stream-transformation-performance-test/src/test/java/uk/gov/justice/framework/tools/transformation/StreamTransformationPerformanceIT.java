package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;


import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidSequenceIdException;

import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

public class StreamTransformationPerformanceIT {

    private static final long STREAMS_COUNT = 100;
    private static final long EVENTS_PER_STREAM = 10;

    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;
    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;
    private final DatabaseUtils databaseUtils = new DatabaseUtils();
    private final SwarmStarterUtil swarmStarterUtil = new SwarmStarterUtil();

    @Ignore("Disabled by default, remove Ignore to run performance test")
    @Test
    public void shouldTransformEventInEventStore() throws Exception {
        insertPerformanceTestData();
        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);

        final long activeStreamsCount = databaseUtils.getEventStreamJdbcRepository().findActive().count();
        assertThat(activeStreamsCount, is(STREAMS_COUNT));

        final long clonedStreamsCount = databaseUtils.getEventStreamJdbcRepository().findAll().count() - activeStreamsCount;
        assertThat(clonedStreamsCount, is(STREAMS_COUNT));

        final long clonedEventsCount = databaseUtils.getEventLogJdbcRepository().findAll()
                .filter(event -> event.getName().equals("system.events.cloned")).count();
        assertThat(clonedEventsCount, is(STREAMS_COUNT));

        final long transformedEventsCount = databaseUtils.getEventLogJdbcRepository().findAll()
                .filter(event -> event.getName().equals("sample.events.transformedName")).count();
        assertThat(transformedEventsCount, is(STREAMS_COUNT * EVENTS_PER_STREAM));
    }


    private void insertPerformanceTestData() throws InvalidPositionException {

        final List<UUID> streamIds = new ArrayList<>();

        for (int i = 0; i < STREAMS_COUNT; i++) {
            streamIds.add(randomUUID());
        }

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        for (final UUID id : streamIds) {
            for (int i = 1; i <= EVENTS_PER_STREAM; i++) {
                Long logId = new Long(i);
                databaseUtils.getEventLogJdbcRepository().insert(eventLogFrom("sample.events.name", logId, id, createdAt));
            }
            databaseUtils.getEventStreamJdbcRepository().insert(id);
        }

    }
}
