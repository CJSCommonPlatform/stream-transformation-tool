package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createReader;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;

import java.io.StringReader;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import javax.json.JsonObject;

import org.junit.Before;
import org.junit.Test;


public class StreamTransformationRollbackIT {

    private static final long STREAM_COUNT_REPORTING_INTERVAL = 10L;
    private static final String MEMORY_OPTIONS_PARAMETER = "2048M";
    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;
    private static final Boolean PROCESS_ALL_STREAMS = true;
    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;

    private static final String EVENT_TO_ANONYMISE = "sample.transformation.anonymise";

    private SwarmStarterUtil swarmStarterUtil;

    private DatabaseUtils databaseUtils;

    @Before
    public void setUp() throws Exception {
        swarmStarterUtil = new SwarmStarterUtil();
        databaseUtils = new DatabaseUtils();
        databaseUtils.dropAndUpdateLiquibase();
    }

    @Test
    public void shouldAnonymiseActiveStreamEventData() throws Exception {

        final UUID activeStreamId = randomUUID();

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData(EVENT_TO_ANONYMISE, activeStreamId, 1L, createdAt);
        databaseUtils.insertEventLogData(EVENT_TO_ANONYMISE, activeStreamId, 3L, createdAt); // deliberately inserting the 2nd event for stream 1 with 3rd position reference

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        final List<Event> events = databaseUtils.getEventLogJdbcRepository().findAll().filter(e -> e.getStreamId().equals(activeStreamId)).collect(toList());

        //this should throw an exception when processing stream as the total number of records and max value of sequence Id do not match. Hence, no transformation should have taken plave
        assertThat(events, hasSize(2));

        final Event event1 = retrieveEvent(activeStreamId, 1l);
        assertNotNull(event1);
        JsonObject payload1 = createReader(new StringReader(event1.getPayload())).readObject();
        assertTrue(payload1.getString("a string").equalsIgnoreCase("test"));

        final Event event2 = retrieveEvent(activeStreamId, 1l);
        assertNotNull(event2);
        JsonObject payload2 = createReader(new StringReader(event2.getPayload())).readObject();
        assertTrue(payload2.getString("a string").equalsIgnoreCase("test"));

    }

    private Event retrieveEvent(final UUID streamId, final long sequenceId) {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        final Optional<Event> event = eventLogs
                .filter(item -> item.getSequenceId().equals(sequenceId))
                .filter(item -> item.getStreamId().equals(streamId))
                .findFirst();

        return event.orElse(null);
    }
}
