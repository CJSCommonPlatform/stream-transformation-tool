package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class StreamTransformer {

    @Inject
    private Logger logger;

    @Inject
    private EventSource eventSource;

    @Inject
    private Enveloper enveloper;

    public Optional<UUID> transformAndBackupStream(final UUID streamId, final Set<EventTransformation> transformations) {
        UUID backupStreamId = null;

        try {
            backupStreamId = eventSource.cloneStream(streamId);

            if (logger.isDebugEnabled()) {
                logger.debug(format("created backup stream '%s' from stream '%s'", backupStreamId, streamId));
            }

            final EventStream stream = eventSource.getStreamById(streamId);
            final Stream<JsonEnvelope> events = stream.read();

            eventSource.clearStream(streamId);

            logger.info("transforming events on stream {}", streamId);

            final Stream<JsonEnvelope> transformedEventStream = transform(events, transformations);

            stream.append(transformedEventStream.map(this::clearEventPositioning));

            events.close();
        } catch (final EventStreamException e) {
            logger.error(format("Failed to backup stream %s", streamId), e);
        } catch (final Exception e) {
            logger.error(format("Unknown error while transforming events on stream %s", streamId), e);
        }
        return ofNullable(backupStreamId);
    }

    private JsonEnvelope clearEventPositioning(final JsonEnvelope event) {
        return enveloper.withMetadataFrom(event, event.metadata().name()).apply(event.payload());
    }

    private Stream<JsonEnvelope> transform(final Stream<JsonEnvelope> events,
                                           final Set<EventTransformation> transformations) {
        return events.map(event -> {
            final Optional<EventTransformation> transformer = hasTransformer(event, transformations);
            return transformer.isPresent() ? transformer.get().apply(event) : Stream.of(event);
        }).flatMap(identity());
    }

    private Optional<EventTransformation> hasTransformer(final JsonEnvelope event,
                                                         final Set<EventTransformation> transformations) {
        return transformations.stream()
                .filter(transformation -> transformation.actionFor(event).isTransform())
                .findFirst();
    }

}
