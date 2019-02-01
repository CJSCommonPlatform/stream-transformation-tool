package uk.gov.justice.framework.tools.transformation;

import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.UUID;

import javax.sql.DataSource;

import liquibase.exception.LiquibaseException;

public class DatabaseUtils {

    private TestEventLogJdbcRepository eventLogJdbcRepository;
    private TestEventStreamJdbcRepository eventStreamJdbcRepository;

    private final LiquibaseUtil liquibaseUtil = new LiquibaseUtil();

    public DatabaseUtils() throws SQLException, LiquibaseException {
        final DataSource dataSource = liquibaseUtil.initEventStoreDb();
        eventLogJdbcRepository = new TestEventLogJdbcRepository(dataSource);
        eventStreamJdbcRepository = new TestEventStreamJdbcRepository(dataSource);
    }

    public void dropAndUpdateLiquibase() throws SQLException, LiquibaseException {
        liquibaseUtil.dropAndUpdate();
    }

    public void resetDatabase() throws SQLException {
        try (final Connection connection = eventLogJdbcRepository.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement("delete from event_log")) {
            preparedStatement.executeUpdate();
        }
        try (final Connection connection = eventStreamJdbcRepository.getDatasource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement("delete from event_stream")) {

            preparedStatement.executeUpdate();
        }
    }

    public void insertEventLogData(final String eventName, final UUID streamId, final long sequenceId, final ZonedDateTime createdAt) throws InvalidPositionException {
        final Event event = eventLogFrom(eventName, sequenceId, streamId, createdAt);

        eventLogJdbcRepository.insert(event);
        eventStreamJdbcRepository.insert(streamId);
    }

    public TestEventLogJdbcRepository getEventLogJdbcRepository() {
        return eventLogJdbcRepository;
    }

    public TestEventStreamJdbcRepository getEventStreamJdbcRepository() {
        return eventStreamJdbcRepository;
    }


}
