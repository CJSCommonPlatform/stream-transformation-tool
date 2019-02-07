package uk.gov.justice.framework.tools.transformation;

import static java.lang.Boolean.valueOf;
import static java.lang.System.getProperty;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;

import java.sql.SQLException;

import liquibase.exception.LiquibaseException;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamTransformationPerformanceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamTransformationPerformanceIT.class);
    private static final String LINE_BREAK = "================================================================================================\n";

    final String EVENT_OR_EVENT_STREAM_COUNT_IS_ZERO = "Test cannot be started, eventsPerStreamCount or eventsStreamCount cannot be set to 0 \n";

    private static final String DEFAULT_MEMORY = "2048Mb";

    private static final String ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY = "false";
    private static final String GENERATE_TEST_DATA = "true";

    private static final int WILDFLY_TIMEOUT_IN_SECONDS = 60;
    private SwarmStarterUtil swarmStarterUtil;
    private DatabaseUtils databaseUtils;

    @Before
    public void setupDatabase() throws Exception {
        swarmStarterUtil = new SwarmStarterUtil();
        databaseUtils = new DatabaseUtils();

    }

    @Ignore("Disabled by default, remove Ignore to run performance test")
    @Test
    public void shouldTransformEventInEventStore() throws Exception {

        generateDataIfRequired();

        final boolean runTest = Boolean.valueOf(getProperty("runTest"));

        LOGGER.info("Run Test: " + runTest);

        if (!runTest) {
            return;
        }

        final long databaseEventStreamCount = Long.valueOf(getProperty("databaseEventStreamCount"));
        final long databaseEventLogCount = Integer.valueOf(getProperty("databaseEventLogCount"));

        final long beforeEventStreamTransform = databaseUtils.getEventStreamJdbcRepository().findAll().count();

        assertThat(LINE_BREAK + "Incorrect event stream count please check data!" + LINE_BREAK, beforeEventStreamTransform,
                is(databaseEventStreamCount));

        final long beforeEventsTransform = databaseUtils.getEventLogJdbcRepository().findAll().count();
        assertThat(LINE_BREAK + "Incorrect event log count please check data!", beforeEventsTransform, is(databaseEventLogCount));

        LOGGER.info("Starting performance test");

        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        LOGGER.info("Stream Count: " + databaseEventStreamCount);
        LOGGER.info("Total events available for transformation: " + databaseEventLogCount);

        final boolean debug = Boolean.valueOf(getProperty("debug", ENABLE_REMOTE_DEBUGGING_FOR_WILDFLY));
        final String memoryOptions = getProperty("memoryOptions", DEFAULT_MEMORY);

        final String streamCountReportingInterval = getProperty("streamCountReportingInterval", "100");
        swarmStarterUtil.runCommand(debug, WILDFLY_TIMEOUT_IN_SECONDS, Long.valueOf(streamCountReportingInterval), memoryOptions);

        stopWatch.stop();

        LOGGER.info("Time taken in millisecs: " + stopWatch.getTime());
        LOGGER.info(LINE_BREAK + "Ending performance test" + LINE_BREAK);

    }

    private void generateDataIfRequired() throws InvalidPositionException, SQLException, LiquibaseException {
        final boolean generateTestData = valueOf(getProperty("generateTestData", GENERATE_TEST_DATA));

        if (generateTestData) {
            final long eventsStreamCount = Long.valueOf(getProperty("eventsStreamCount"));
            final long eventsPerStreamCount = Integer.valueOf(getProperty("eventsPerStreamCount"));
            final long generatedTestDataReportingInterval = Integer.valueOf(getProperty("generatedTestDataReportingInterval"));

            if (eventsPerStreamCount == 0l || eventsStreamCount == 0l) {
                LOGGER.info(LINE_BREAK);
                LOGGER.info(EVENT_OR_EVENT_STREAM_COUNT_IS_ZERO);
                LOGGER.info(LINE_BREAK);
                return;
            }
            LOGGER.info("Generating Test data");
            LOGGER.info("Events Per Stream: " + eventsStreamCount);
            LOGGER.info("Streams: " + eventsStreamCount);
            GenerateTestData.generateTestData(generatedTestDataReportingInterval, eventsStreamCount, eventsPerStreamCount);
        }
    }
}
