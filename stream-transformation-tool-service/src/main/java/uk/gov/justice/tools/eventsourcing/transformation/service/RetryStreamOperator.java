package uk.gov.justice.tools.eventsourcing.transformation.service;

import uk.gov.justice.services.eventsourcing.source.core.EventSourceTransformation;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class RetryStreamOperator {

    @Inject
    private Logger logger;

    @Inject
    private StreamOperationRetryableExecutor streamOperationRetryableExecutor;

    @Inject
    private EventSourceTransformation eventSourceTransformation;

    public void appendWithRetry(final UUID streamId, final EventStream eventStream, final Stream<JsonEnvelope> events) throws EventStreamException {
        streamOperationRetryableExecutor.execute(streamId, () -> {
            eventStream.append(events);
            logger.info("Appended events to stream with ID - '{}'", streamId);
        });

    }

    public void cloneWithRetry(final UUID streamId) throws EventStreamException {
        streamOperationRetryableExecutor.execute(streamId, () -> {
            final UUID clonedStreamId = eventSourceTransformation.cloneStream(streamId);
            logger.info("Created backup stream '{}' from stream '{}'", clonedStreamId, streamId);
        });
    }

}
