package uk.gov.justice.framework.tools.transformation;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidSequenceIdException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class StreamTransformationPerformanceIT {

    private static final long STREAMS_COUNT = 100;
    private static final long EVENTS_PER_STREAM = 10;

    private static TestEventLogJdbcRepository EVENT_LOG_JDBC_REPOSITORY;

    private static TestEventStreamJdbcRepository EVENT_STREAM_JDBC_REPOSITORY;

    private DataSource dataSource;

    private SwarmStarterUtil swarmStarterUtil = new SwarmStarterUtil();

    private LiquibaseUtil liquibaseUtil = new LiquibaseUtil();


    @Before
    public void setUpDB() throws Exception {
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

    @Ignore("Disabled by default, remove Ignore to run performance test")
    @Test
    public void shouldTransformEventInEventStore() throws Exception {
        insertPerformanceTestData();
        swarmStarterUtil.runCommand();

        final long activeStreamsCount = EVENT_STREAM_JDBC_REPOSITORY.findActive().count();
        assertThat(activeStreamsCount, is(STREAMS_COUNT));

        final long clonedStreamsCount = EVENT_STREAM_JDBC_REPOSITORY.findAll().count() - activeStreamsCount;
        assertThat(clonedStreamsCount, is(STREAMS_COUNT));

        final long clonedEventsCount = EVENT_LOG_JDBC_REPOSITORY.findAll()
                .filter(event -> event.getName().equals("system.events.cloned")).count();
        assertThat(clonedEventsCount, is(STREAMS_COUNT));

        final long transformedEventsCount = EVENT_LOG_JDBC_REPOSITORY.findAll()
                .filter(event -> event.getName().equals("sample.events.transformedName")).count();
        assertThat(transformedEventsCount, is(STREAMS_COUNT * EVENTS_PER_STREAM));
    }


    private void insertPerformanceTestData() throws SQLException, InvalidSequenceIdException {

        final List<UUID> streamIds = new ArrayList<>();

        for (int i = 0; i < STREAMS_COUNT; i++) {
            streamIds.add(randomUUID());
        }

        for (UUID id : streamIds) {
            for (int i = 1; i <= EVENTS_PER_STREAM; i++) {
                Long logId = new Long(i);
                EVENT_LOG_JDBC_REPOSITORY.insert(eventLogFrom("sample.events.name", logId, id));
            }
            EVENT_STREAM_JDBC_REPOSITORY.insert(id);

        }

    }
}
