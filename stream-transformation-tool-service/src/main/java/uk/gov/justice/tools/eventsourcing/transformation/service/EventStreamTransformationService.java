package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.util.function.Function.identity;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Service to transform events contained on an event-stream.
 */
public class EventStreamTransformationService {

    @Inject
    EventSource eventSource;

    Set<EventTransformation> transformations;

    public void transformEventStream(final Stream<JsonEnvelope> eventStream) throws EventStreamException {
        final Optional<UUID> streamId = requiresTransformation(eventStream);

        if (streamId.isPresent()) {
            final UUID eventStreamId = streamId.get();

            final UUID clonedStreamId = eventSource.cloneStream(eventStreamId);
            eventSource.clearStream(eventStreamId);

            final EventStream stream = eventSource.getStreamById(clonedStreamId);

            final Stream<JsonEnvelope> transformedEventStream = transform(stream.read());

            final EventStream originalStream = eventSource.getStreamById(eventStreamId);
            originalStream.append(transformedEventStream);
        }
    }

    private Stream<JsonEnvelope> transform(final Stream<JsonEnvelope> eventStream) {
        return eventStream.map(e -> {
                    final Optional<EventTransformation> transformer = hasTransformer(e);
                    return transformer.isPresent() ? transformer.get().apply(e) : Stream.of(e);
                }
        ).flatMap(identity());
    }

    private Optional<UUID> requiresTransformation(final Stream<JsonEnvelope> eventStream) {
        return eventStream
                .filter(this::checkTransformations)
                .map(e -> e.metadata().streamId().get())
                .findFirst();
    }

    private Optional<EventTransformation> hasTransformer(final JsonEnvelope event) {
        return transformations.stream().filter(t -> t.isApplicable(event)).findFirst();
    }

    private boolean checkTransformations(final JsonEnvelope event) {
        return transformations.stream().anyMatch(t -> t.isApplicable(event));
    }
}
