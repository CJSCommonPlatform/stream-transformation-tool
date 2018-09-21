package uk.gov.justice.framework.tools.transformation;

import static uk.gov.justice.services.test.utils.common.reflection.ReflectionUtils.setField;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryHelper;

import javax.sql.DataSource;

public class TestEventStreamJdbcRepository extends EventStreamJdbcRepository {

    private final DataSource dbsource;

    public TestEventStreamJdbcRepository(final DataSource dataSource) {
        this.dbsource = dataSource;
        setField(this, "dataSource", dbsource);
        setField(this, "eventStreamJdbcRepositoryHelper", new JdbcRepositoryHelper());
        setField(this, "clock", new UtcClock());
    }

    public DataSource getDatasource() {
        return dbsource;
    }
}
