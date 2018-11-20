package uk.gov.justice.framework.tools.transformation;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;

public class DatabaseUtils {
    private static final Logger LOGGER = getLogger(DatabaseUtils.class);

    private TestEventLogJdbcRepository eventLogJdbcRepository;
    private TestEventStreamJdbcRepository eventStreamJdbcRepository;

    private final LiquibaseUtil liquibaseUtil = new LiquibaseUtil();

    public DatabaseUtils() {
        try {
            final DataSource dataSource = liquibaseUtil.initEventStoreDb();
            eventLogJdbcRepository = new TestEventLogJdbcRepository(dataSource);
            eventStreamJdbcRepository = new TestEventStreamJdbcRepository(dataSource);
        } catch (final Exception e) {
            LOGGER.error(format("Failed to create database connections '%s'", e.getMessage()));
        }
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

    public void insertEventLogData(final String eventName, final UUID streamId, final long sequenceId) throws InvalidPositionException {
        eventLogJdbcRepository.insert(eventLogFrom(eventName, sequenceId, streamId));
        eventStreamJdbcRepository.insert(streamId);
    }

    public TestEventLogJdbcRepository getEventLogJdbcRepository() {
        return eventLogJdbcRepository;
    }

    public TestEventStreamJdbcRepository getEventStreamJdbcRepository() {
        return eventStreamJdbcRepository;
    }


}
