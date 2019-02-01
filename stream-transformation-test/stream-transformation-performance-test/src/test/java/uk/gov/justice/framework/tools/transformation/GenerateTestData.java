package uk.gov.justice.framework.tools.transformation;

import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.UUID.randomUUID;
import static uk.gov.justice.framework.tools.transformation.EventLogBuilder.eventLogFrom;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;

import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import liquibase.exception.LiquibaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class by default creates 1000 events with 100 streams with 10 events per stream These can be
 * configured by supplying the right system properties  example as below -DeventStreamSize=1000
 * -DeventsPerStreamSize=10 -DgeneratedTestDataReportingInterval=200
 *
 * This will create 10000 event log entries and when it runs will report progress for each pass
 * after every 200th event
 */
public class GenerateTestData {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateTestData.class);

    private static final String DEFAULT_STREAMS_COUNT = "100";
    private static final String DEFAULT_EVENTS_PER_STREAM = "10";
    private static final String DEFAULT_GENERATED_TEST_DATA_REPORTING_INTERVAL = "10";

    public static void main(String[] args) throws LiquibaseException, InvalidPositionException, SQLException {
        final long eventStreamSize = Long.valueOf(getProperty("eventStreamSize", DEFAULT_STREAMS_COUNT));
        final long eventsPerStreamSize = Long.valueOf(getProperty("eventsPerStreamSize", DEFAULT_EVENTS_PER_STREAM));
        final long testDataCountStepInfo = Long.valueOf(getProperty("generatedTestDataReportingInterval", DEFAULT_GENERATED_TEST_DATA_REPORTING_INTERVAL));

        LOGGER.info("Events Per Stream: " + eventsPerStreamSize);
        LOGGER.info("Streams: " + eventStreamSize);

        GenerateTestData.generateTestData(testDataCountStepInfo, eventStreamSize, eventsPerStreamSize);

        LOGGER.info("Generation of Test Data Completed");
    }

    public static void generateTestData(final long generatedTestDataReportingInterval,
                                        final long eventStreamSize, final long eventsPerStreamSize) throws InvalidPositionException, SQLException, LiquibaseException {
        final DatabaseUtils databaseUtils = new DatabaseUtils();
        databaseUtils.dropAndUpdateLiquibase();
        final List<UUID> streamIds = new ArrayList<>();

        for (int i = 0; i < eventStreamSize; i++) {
            streamIds.add(randomUUID());
        }
        final ZonedDateTime createdAt = new UtcClock().now().minusMonths(1);

        int streamCount = 0;
        for (final UUID id : streamIds) {
            streamCount++;
            int eventCount = 0;
            for (long logId = 1; logId <= eventsPerStreamSize; logId++) {
                eventCount++;
                databaseUtils.getEventLogJdbcRepository().insert(eventLogFrom("sample.events.name", logId, id, createdAt));
                if (eventCount % generatedTestDataReportingInterval == 0) {
                    LOGGER.info(format("%s Events created for streams created %s", eventCount, streamCount));
                }
            }
            databaseUtils.getEventStreamJdbcRepository().insert(id);
        }
    }
}
