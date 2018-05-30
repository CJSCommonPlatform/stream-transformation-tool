package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.ARCHIVE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
    EventSource eventSource;

    @Inject
    Enveloper enveloper;

    @Inject
    EventStreamJdbcRepository eventStreamJdbcRepository;

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

        switch (requiresTransformation(eventStream, streamId)) {
            case TRANSFORM:
                return transformEvent(streamId, eventStream);
            case NO_ACTION:
                return null;
            case ARCHIVE:
                return archiveStream(streamId, eventStream);
        }
        return null;
    }


    private UUID transformEvent(UUID streamId, Stream<JsonEnvelope> eventStream) {
        try {
            final UUID clonedStreamId = eventSource.cloneStream(streamId);

            if (logger.isDebugEnabled()) {
                logger.debug(format("Cloned stream '%s' from stream '%s'", clonedStreamId, streamId));
            }

            final EventStream stream = eventSource.getStreamById(streamId);
            final Stream<JsonEnvelope> events = stream.read();

            eventSource.clearStream(streamId);

            logger.info("transforming events on stream {}", streamId);
            final Stream<JsonEnvelope> transformedEventStream = transform(events);

            stream.append(transformedEventStream.map(this::clearEventVersion));
            events.close();
        } catch (Exception e) {
            logger.error("Failed to clone stream {}", streamId, e);
        }
        eventStream.close();
        return streamId;

    }

    private UUID archiveStream(final UUID streamId, final Stream<JsonEnvelope> eventStream) throws EventStreamException {
        eventStreamJdbcRepository.markActive(streamId, false);
        eventStream.close();
        return streamId;

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

    private TransformAction requiresTransformation(final Stream<JsonEnvelope> eventStream, final UUID streamId) {

        List<TransformAction> eventTransformationList = eventStream.map(t -> checkTransformations(t))
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        if (eventTransformationList.isEmpty()) {
            return noAction(eventStream, streamId, "Stream {} did not require transformation stream ", eventTransformationList);
        }
        if (eventTransformationList.size() > 1) {
            return noAction(eventStream, streamId, "Stream {} can not have multiple actions {} ", eventTransformationList);
        }

        return eventTransformationList.get(0);

    }

    private TransformAction noAction(final Stream<JsonEnvelope> eventStream, final UUID streamId, final String errorMessage, final List<TransformAction> eventTransformationList) {
        logger.debug(errorMessage, streamId, eventTransformationList.toString());
        eventStream.close();
        return NO_ACTION;
    }

    private Optional<EventTransformation> hasTransformer(final JsonEnvelope event) {
        return transformations.stream().filter(t -> t.actionFor(event) == TRANSFORM).findFirst();
    }

    private List<TransformAction> checkTransformations(final JsonEnvelope event) {

        return transformations.stream()
                .map(t -> t.actionFor(event))
                .filter(t -> t == TRANSFORM || t == ARCHIVE)
                .collect(Collectors.toList());
    }
}
