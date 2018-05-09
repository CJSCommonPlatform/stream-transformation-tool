package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.EventStreamManager;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

/**
 * Service to transform events on an event-stream.
 */
@ApplicationScoped
public class EventStreamTransformationService {

    @Inject
    Logger logger;

    @Inject
    EventStreamManager eventStreamManager;

    @Inject
    EventSource eventSource;

    @Inject
    Enveloper enveloper;

    Set<EventTransformation> transformations = new HashSet<>();

    /**
     * Register method, invoked automatically to register all {@link EventTransformation} classes
     * into the transformations set.
     *
     * @param event identified by the framework to be registered into the event map.
     */
    public void register(@Observes final EventTransformationFoundEvent event) throws IllegalAccessException, InstantiationException {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Loading Event Transformation %s", event.getClazz().getSimpleName()));
        }

        final EventTransformation et = (EventTransformation) event.getClazz().newInstance();
        et.setEnveloper(enveloper);
        transformations.add(et);
    }

    @Transactional(REQUIRES_NEW)
    public UUID transformEventStream(final UUID streamId) throws EventStreamException {
        final Stream<JsonEnvelope> eventStream = eventSource.getStreamById(streamId).read();

        if (requiresTransformation(eventStream)) {
            try {
                final UUID clonedStreamId = eventStreamManager.cloneAsAncestor(streamId);

                if (logger.isDebugEnabled()) {
                    logger.debug(format("Cloned stream '%s' from stream '%s'", clonedStreamId, streamId));
                }

                final EventStream stream = eventSource.getStreamById(streamId);
                final Stream<JsonEnvelope> events = stream.read();

                eventStreamManager.clear(streamId);

                logger.info("transforming events on stream {}", streamId);
                final Stream<JsonEnvelope> transformedEventStream = transform(events);

                stream.append(transformedEventStream.map(this::clearEventVersion));
                events.close();
            } catch (Exception e) {
                logger.error("Failed to clone stream", e);
            }
            eventStream.close();
            return streamId;
        } else {
            logger.debug("Stream {} did not require transformation", streamId);
            eventStream.close();
            return null;
        }
    }

    private JsonEnvelope clearEventVersion(final JsonEnvelope event) {
        return enveloper.withMetadataFrom(event, event.metadata().name()).apply(event.payload());
    }

    private Stream<JsonEnvelope> transform(final Stream<JsonEnvelope> eventStream) {
        return eventStream.map(e -> {
                    final Optional<EventTransformation> transformer = hasTransformer(e);
                    return transformer.isPresent() ? transformer.get().apply(e) : Stream.of(e);
                }
        ).flatMap(identity());
    }

    private boolean requiresTransformation(final Stream<JsonEnvelope> eventStream) {
        return eventStream.filter(this::checkTransformations).count() > 0;
    }

    private Optional<EventTransformation> hasTransformer(final JsonEnvelope event) {
        return transformations.stream().filter(t -> t.isApplicable(event)).findFirst();
    }

    private boolean checkTransformations(final JsonEnvelope event) {
        return transformations.stream().anyMatch(t -> t.isApplicable(event));
    }
}
