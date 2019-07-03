package uk.gov.justice.tools.eventsourcing.transformation.service;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EnvelopeFixer;

import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class StreamAppender {

    @Inject
    private EventSource eventSource;

    @Inject
    private EnvelopeFixer envelopeFixer;

    @Inject
    private RetryStreamOperator retryStreamOperator;

    public void appendEventsToStream(
            final UUID streamId,
            final Stream<JsonEnvelope> jsonEnvelopeStream) throws EventStreamException {

        final EventStream eventStream = eventSource.getStreamById(streamId);
        final Stream<JsonEnvelope> events = jsonEnvelopeStream.map(envelopeFixer::clearPositionAndGiveNewId);
        retryStreamOperator.appendWithRetry(streamId, eventStream, events);
    }

}
