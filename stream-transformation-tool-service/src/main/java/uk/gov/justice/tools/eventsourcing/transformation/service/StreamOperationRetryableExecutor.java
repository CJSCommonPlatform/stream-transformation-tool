package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;

import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.util.Sleeper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;

import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class StreamOperationRetryableExecutor {

    private static final int SLEEP_TIME_IN_MILLISECONDS = 2000;

    @Inject
    @Value(key = "maxRetryStreamOperation", defaultValue = "10")
    private long maxRetry;

    @Inject
    private Logger logger;

    @Inject
    private Sleeper sleeper;

    public void execute(final UUID streamId, final StreamOperationRetryable streamOperationRetryable) throws EventStreamException {
        boolean operationCompletedSuccesfully = false;
        long retryCount = 0L;
        while (!operationCompletedSuccesfully) {
            try {
                streamOperationRetryable.execute();
                operationCompletedSuccesfully = true;
            } catch (OptimisticLockingRetryException e) {
                retryCount++;
                if (retryCount >= maxRetry) {
                    logger.error("Failed to complete operation on stream '{}' due to concurrency issues. Exhausted all retries.", streamId);
                    throw e;
                }
                sleeper.sleepFor(SLEEP_TIME_IN_MILLISECONDS);
                logger.warn(format("Encountered exception whilst completing operation on stream during attempt %s for stream with ID: %s", retryCount, streamId), e);
            }
        }
    }
}
