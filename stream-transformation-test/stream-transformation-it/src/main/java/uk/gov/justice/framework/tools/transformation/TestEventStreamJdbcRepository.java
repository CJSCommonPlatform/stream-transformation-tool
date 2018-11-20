package uk.gov.justice.framework.tools.transformation;

import static org.slf4j.LoggerFactory.getLogger;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryHelper;

import javax.sql.DataSource;

public class TestEventStreamJdbcRepository extends EventStreamJdbcRepository {

    private final DataSource dbsource;

    public TestEventStreamJdbcRepository(final DataSource dataSource) {
        super(
                new JdbcRepositoryHelper(),
                jndiName -> dataSource,
                new UtcClock(),
                "",
                getLogger(EventStreamJdbcRepository.class)
        );

        this.dbsource = dataSource;
    }

    public DataSource getDatasource() {
        return dbsource;
    }
}
