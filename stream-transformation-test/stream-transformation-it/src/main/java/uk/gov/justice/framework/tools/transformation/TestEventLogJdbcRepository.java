package uk.gov.justice.framework.tools.transformation;


import static org.slf4j.LoggerFactory.getLogger;

import uk.gov.justice.services.eventsourcing.repository.jdbc.AnsiSQLEventLogInsertionStrategy;
import uk.gov.justice.services.eventsourcing.repository.jdbc.PostgresSQLEventLogInsertionStrategy;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryHelper;

import javax.sql.DataSource;

/**
 * Standalone repository class to access event streams. To be used in integration testing
 */
public class TestEventLogJdbcRepository extends EventJdbcRepository {

    protected final DataSource dbsource;

    public TestEventLogJdbcRepository(final DataSource datasource) {
        super(
                new PostgresSQLEventLogInsertionStrategy(),
                new JdbcRepositoryHelper(),
                jndiName -> datasource,
                "",
                getLogger(EventJdbcRepository.class)
        );

        this.dbsource = datasource;
    }

    protected DataSource getDataSource() {
        return dbsource;
    }
}
