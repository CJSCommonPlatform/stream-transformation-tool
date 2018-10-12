package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamTransformationMoveIT {

    private static final UUID STREAM_ID = randomUUID();

    private static final Boolean ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = true;

    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 600;

    private final SwarmStarterUtil swarmStarterUtil = new SwarmStarterUtil();

    private DatabaseUtils databaseUtils;


    @Before
    public void setUp() throws Exception {
        databaseUtils = new DatabaseUtils();
    }

    @After
    public void cleanup() throws Exception {
        databaseUtils.resetDatabase();
    }

//    @Test
//    public void shouldMoveEventInEventStore() throws Exception {
//        databaseUtils.insertEventLogData("sample.transformation.move.1", STREAM_ID, 1L);
//        databaseUtils.insertEventLogData("sample.events.name.passer", STREAM_ID, 2L);
//
//        swarmStarterUtil.runCommand(ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY, WILDFLY_TIMEOUT_IN_SECONDS);
//
//        assertThat(totalStreamCount(), is(4L));
//        assertThat(totalClonedStreamsCreated(), is(2L));
//        assertTrue(eventNameExist("sample.transformation.move.3"));
//    }

    @Test
    public void shouldMoveEventInEventStoreWithoutBackup() throws Exception {
        databaseUtils.insertEventLogData("sample.transformation.move.without.backup", STREAM_ID, 1L);
        databaseUtils.insertEventLogData("sample.events.name.passer", STREAM_ID, 2L);

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
        databaseUtils.getEventLogJdbcRepository().findAll().forEach(event -> System.out.println(event.toString()));

        System.out.println("Streams found: " + databaseUtils.getEventStreamJdbcRepository().findAll().count());

        return databaseUtils.getEventStreamJdbcRepository().findAll().count();
    }

    private long totalClonedStreamsCreated() {
        final Stream<Event> eventStream = databaseUtils.getEventLogJdbcRepository()
                .findAll()
                .filter(event -> event.getName().equals("system.events.cloned"));

        return eventStream.count();
    }

}
