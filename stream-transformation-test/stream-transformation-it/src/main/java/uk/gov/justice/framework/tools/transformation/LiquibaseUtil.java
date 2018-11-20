package uk.gov.justice.framework.tools.transformation;

import java.sql.SQLException;

import javax.sql.DataSource;

import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.postgresql.ds.PGSimpleDataSource;

public class LiquibaseUtil {

    private static final String DATABASE_NAME = "frameworkeventstore";

    private TestProperties testProperties = new TestProperties("test.properties");

    public DataSource initEventStoreDb() throws SQLException, LiquibaseException {

        final String username = testProperties.value("db.eventstore.userName");
        final String password = testProperties.value("db.eventstore.password");
        final int portNumber = Integer.parseInt(testProperties.value("db.eventstore.portNumber"));

        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setPortNumber(portNumber);
        dataSource.setDatabaseName(DATABASE_NAME);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        final JdbcConnection jdbcConnection = new JdbcConnection(dataSource.getConnection());

        final Liquibase liquibase = new Liquibase(
                "liquibase/event-store-db-changelog.xml",
                new ClassLoaderResourceAccessor(),
                jdbcConnection);

        liquibase.dropAll();
        liquibase.update("");

        return dataSource;
    }
}
