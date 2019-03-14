package uk.gov.justice.framework.tools.transformation;

import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.eventsourcing.linkedevent.LinkedEventJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryHelper;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.sql.DataSource;

public class TestLinkedEventJdbcRepository extends LinkedEventJdbcRepository {
    private static final String SQL_FIND_BY_STREAM_ID = "SELECT * FROM linked_event WHERE stream_id=? ORDER BY position_in_stream ASC";

    private final DataSource dbsource;
    private static final String COL_ID = "id";
    private static final String COL_STREAM_ID = "stream_id";
    private static final String COL_POSITION = "position_in_stream";
    private static final String COL_EVENT_NUMBER = "event_number";
    private static final String COL_PREVIOUS_EVENT_NUMBER = "previous_event_number";
    private static final String COL_DATE_CREATED = "date_created";

    public TestLinkedEventJdbcRepository(final DataSource dataSource) {
        this.dbsource = dataSource;
    }

    public DataSource getDatasource() {
        return dbsource;
    }


    public Stream<LinkedEvent> findByStreamIdOrderByPositionAsc(final UUID streamId) throws SQLException {
        final JdbcRepositoryHelper jdbcRepositoryHelper = new JdbcRepositoryHelper();

        final PreparedStatementWrapper preparedStatementWrapper = jdbcRepositoryHelper.preparedStatementWrapperOf(getDatasource(), SQL_FIND_BY_STREAM_ID);
        preparedStatementWrapper.setObject(1, streamId);

        return jdbcRepositoryHelper.streamOf(preparedStatementWrapper, entityFromFunction1());
    }



    public long linkedEventsCount(final UUID streamId) throws SQLException {
        final JdbcRepositoryHelper jdbcRepositoryHelper = new JdbcRepositoryHelper();

        final PreparedStatementWrapper preparedStatementWrapper = jdbcRepositoryHelper.preparedStatementWrapperOf(getDatasource(), SQL_FIND_BY_STREAM_ID);
        preparedStatementWrapper.setObject(1, streamId);

        return jdbcRepositoryHelper.streamOf(preparedStatementWrapper, entityFromFunction1()).count();
    }

    protected Function<ResultSet, LinkedEvent> entityFromFunction1() {
        return resultSet -> {
            try {
                return new LinkedEvent((UUID) resultSet.getObject(COL_ID),
                        (UUID) resultSet.getObject(COL_STREAM_ID),
                        resultSet.getLong(COL_POSITION),
                        resultSet.getString("name"),
                        resultSet.getString("payload"),
                        resultSet.getString("metadata"),
                        fromSqlTimestamp(resultSet.getTimestamp(COL_DATE_CREATED)),
                        resultSet.getLong(COL_EVENT_NUMBER),
                        resultSet.getLong(COL_PREVIOUS_EVENT_NUMBER));
            } catch (final SQLException e) {
                throw new JdbcRepositoryException(e);
            }
        };
    }
}
