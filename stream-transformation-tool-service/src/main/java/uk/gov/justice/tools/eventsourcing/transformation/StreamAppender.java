package uk.gov.justice.tools.eventsourcing.transformation;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;

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

    public void appendEventsToStream(
            final UUID streamId,
            final Stream<JsonEnvelope> jsonEnvelopeStream) throws EventStreamException {

        try {
            final EventStream eventStream = eventSource
                    .getStreamById(streamId);
            eventStream
                    .append(jsonEnvelopeStream.map(envelopeFixer::clearPositionAndGiveNewId));
        } catch (final Exception e) {
            jsonEnvelopeStream.close();
            throw new EventStreamException("Failed to append events to stream", e);
        }
    }
}
