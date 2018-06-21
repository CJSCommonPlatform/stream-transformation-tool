package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
    private Logger logger;

    @Inject
    private EventSource eventSource;

    @Inject
    private Enveloper enveloper;

    @Inject
    private StreamTransformer streamTransformer;

    @Inject
    private StreamRepository streamRepository;

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

        final EventTransformation eventTransformation = (EventTransformation) event.getClazz().newInstance();
        eventTransformation.setEnveloper(enveloper);
        transformations.add(eventTransformation);
    }

    @Transactional(REQUIRES_NEW)
    public UUID transformEventStream(final UUID streamId) {
        final Stream<JsonEnvelope> eventStream = eventSource.getStreamById(streamId).read();
        final Action action = requiresTransformation(eventStream, streamId);

        Optional<UUID> backupStreamId;

        if (action.isTransform()) {
            backupStreamId = streamTransformer.transformAndBackupStream(streamId, transformations);

            if (!action.isKeepBackup()) {
                if (backupStreamId.isPresent()) {
                    streamRepository.deleteStream(backupStreamId.get());
                } else {
                    if (logger.isWarnEnabled()) {
                        logger.warn(format("cannot delete backup stream. No backup stream was created for stream '%s'", streamId));
                    }
                }
            }
        }

        if (action.isDeactivate()) {
            streamRepository.deactivateStream(streamId);
        }

        eventStream.close();
        return streamId;
    }

    private Action requiresTransformation(final Stream<JsonEnvelope> eventStream, final UUID streamId) {
        final List<Action> eventTransformationList = eventStream.map(this::checkTransformations)
                .flatMap(List::stream)
                .distinct()
                .collect(toList());

        if (eventTransformationList.isEmpty()) {
            return noAction(streamId, "Stream {} did not require transformation stream ", eventTransformationList);
        }
        if (eventTransformationList.size() > 1) {
            return noAction(streamId, "Stream {} can not have multiple actions {} ", eventTransformationList);
        }

        return eventTransformationList.get(0);
    }

    private Action noAction(final UUID streamId, final String errorMessage,
                            final List<Action> eventTransformationList) {
        if (logger.isDebugEnabled()) {
            logger.debug(errorMessage, streamId, eventTransformationList.toString());
        }
        return NO_ACTION;
    }

    private List<Action> checkTransformations(final JsonEnvelope event) {
        return transformations.stream()
                .map(t -> t.actionFor(event))
                .filter(Objects::nonNull)
                .filter(t -> !t.equals(NO_ACTION))
                .collect(toList());
    }
}
