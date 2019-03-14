package uk.gov.justice.framework.tools.transformation;

import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;

import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.UUID;

import javax.sql.DataSource;

import liquibase.exception.LiquibaseException;

public class DatabaseUtils {

    private final TestEventLogJdbcRepository eventLogJdbcRepository;
    private final TestEventStreamJdbcRepository eventStreamJdbcRepository;
    private final LiquibaseUtil liquibaseUtil = new LiquibaseUtil();
    private final DataSource dataSource;

    public DatabaseUtils() throws SQLException, LiquibaseException {
        dataSource = liquibaseUtil.initEventStoreDb();
        eventLogJdbcRepository = new TestEventLogJdbcRepository(dataSource);
        eventStreamJdbcRepository = new TestEventStreamJdbcRepository(dataSource);
    }

    public DataSource getDataSource() {
         return dataSource;
    }

    public void dropAndUpdateLiquibase() throws SQLException, LiquibaseException {
        liquibaseUtil.dropAndUpdate();
    }

    public void insertEventLogData(final String eventName, final UUID streamId, final long sequenceId, final ZonedDateTime createdAt) throws InvalidPositionException {
        final Event event = eventLogFrom(eventName, sequenceId, streamId, createdAt);

        eventLogJdbcRepository.insert(event);
        eventStreamJdbcRepository.insert(streamId);
    }

    public void insertEventLogData(final String eventName, final UUID streamId, final long sequenceId, final ZonedDateTime createdAt, final long eventNumber) throws InvalidPositionException {
        final Event event = eventLogFrom(eventName, sequenceId, streamId, createdAt, eventNumber);

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
