package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;

import java.io.StringReader;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.Before;
import org.junit.Test;


public class StreamAnonymisationTransformationIT {

    private static final long STREAM_COUNT_REPORTING_INTERVAL = 10L;
    private static final String MEMORY_OPTIONS_PARAMETER = "2048M";
    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = false;
    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;

    private static final UUID STREAM_ID = randomUUID();
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
    public void shouldAnonymiseEventData() throws Exception {

        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        databaseUtils.insertEventLogData(EVENT_TO_ANONYMISE, STREAM_ID, 1L, createdAt);

        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS, STREAM_COUNT_REPORTING_INTERVAL, MEMORY_OPTIONS_PARAMETER);

        final List<Event> events = databaseUtils.getEventLogJdbcRepository().findAll().filter(e -> e.getStreamId().equals(STREAM_ID)).collect(toList());

        assertThat(events, hasSize(1));

        final Event event = retrieveEvent(STREAM_ID, EVENT_TO_ANONYMISE);
        assertNotNull(event);
        JsonObject payload = Json.createReader(new StringReader(event.getPayload())).readObject();
        assertFalse(payload.getString("a string").equalsIgnoreCase("test"));
        assertThat(payload.getString("a string").length(), is("test".length()));

    }

    private Event retrieveEvent(final UUID streamId, final String eventName) {
        final Stream<Event> eventLogs = databaseUtils.getEventLogJdbcRepository().findAll();
        final Optional<Event> event = eventLogs
                .filter(item -> item.getName().equals(eventName))
                .filter(item -> item.getStreamId().equals(streamId))
                .findFirst();

        return event.orElse(null);
    }
}
