package uk.gov.justice.framework.tools.replay;

import static uk.gov.justice.services.test.utils.common.reflection.ReflectionUtils.setField;

import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryHelper;

import javax.sql.DataSource;

public class TestEventStreamJdbcRepository extends EventStreamJdbcRepository {

    private final DataSource datasource;

    public TestEventStreamJdbcRepository(DataSource dataSource) {
        this.datasource = dataSource;
        setField(this, "dataSource", datasource);
        setField(this, "eventStreamJdbcRepositoryHelper", new JdbcRepositoryHelper());

    }

    public DataSource getDatasource() {
        return datasource;
    }
}
