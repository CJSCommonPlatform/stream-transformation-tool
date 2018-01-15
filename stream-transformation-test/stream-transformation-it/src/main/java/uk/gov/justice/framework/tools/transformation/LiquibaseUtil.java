package uk.gov.justice.framework.tools.transformation;

import java.sql.SQLException;

import javax.sql.DataSource;

import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.dbcp2.BasicDataSource;

public class LiquibaseUtil {

    private static final TestProperties TEST_PROPERTIES = new TestProperties("test.properties");

    public DataSource initEventStoreDb() throws SQLException, LiquibaseException {
        return initDatabase("db.eventstore.url",
                "db.eventstore.userName",
                "db.eventstore.password",
                "liquibase/event-store-db-changelog.xml");
    }

    private DataSource initDatabase(final String dbUrlPropertyName,
                                    final String dbUserNamePropertyName,
                                    final String dbPasswordPropertyName,
                                    final String... liquibaseChangeLogXmls) throws SQLException, LiquibaseException {

        final BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriverClassName(TEST_PROPERTIES.value("db.driver"));

        dataSource.setUrl(TEST_PROPERTIES.value(dbUrlPropertyName));
        dataSource.setUsername(TEST_PROPERTIES.value(dbUserNamePropertyName));
        dataSource.setPassword(TEST_PROPERTIES.value(dbPasswordPropertyName));
        boolean dropped = false;
        final JdbcConnection jdbcConnection = new JdbcConnection(dataSource.getConnection());

        for (String liquibaseChangeLogXml : liquibaseChangeLogXmls) {
            Liquibase liquibase = new Liquibase(liquibaseChangeLogXml,
                    new ClassLoaderResourceAccessor(), jdbcConnection);
            if (!dropped) {
                liquibase.dropAll();
                dropped = true;
            }
            liquibase.update("");
        }
        return dataSource;
    }


}
