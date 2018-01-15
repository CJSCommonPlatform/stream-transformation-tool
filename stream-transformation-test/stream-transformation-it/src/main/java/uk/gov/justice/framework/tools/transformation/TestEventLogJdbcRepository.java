package uk.gov.justice.framework.tools.transformation;


import static uk.gov.justice.services.test.utils.common.reflection.ReflectionUtils.setField;

import uk.gov.justice.services.eventsourcing.repository.jdbc.AnsiSQLEventLogInsertionStrategy;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryHelper;

import javax.sql.DataSource;

/**
 * Standalone repository class to access event streams. To be used in integration testing
 */
public class TestEventLogJdbcRepository extends EventJdbcRepository {

    protected final DataSource dbsource;

    public TestEventLogJdbcRepository(final DataSource datasource) {
        this.dbsource = datasource;
        setField(this, "eventInsertionStrategy", new AnsiSQLEventLogInsertionStrategy());
        setField(this, "dataSource", datasource);
        setField(this, "jdbcRepositoryHelper", new JdbcRepositoryHelper());
    }

    protected DataSource getDataSource() {
        return dbsource;
    }
}
