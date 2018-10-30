package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventStreamReader;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationStreamIdFilter;
import uk.gov.justice.tools.eventsourcing.transformation.StreamMover;
import uk.gov.justice.tools.eventsourcing.transformation.TransformationChecker;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
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
    private StreamTransformer streamTransformer;

    @Inject
    private StreamMover streamMover;

    @Inject
    private StreamRepository streamRepository;

    @Inject
    private EventTransformationRegistry eventTransformationRegistry;

    @Inject
    private TransformationChecker transformationChecker;

    @Inject
    private EventTransformationStreamIdFilter eventTransformationStreamIdFilter;

    @Inject
    private EventStreamReader eventStreamReader;


    @Transactional(REQUIRES_NEW)
    public UUID transformEventStream(final UUID originalStreamId, final int pass) {
        try {

            final List<JsonEnvelope> jsonEnvelopeList = eventStreamReader.getStreamBy(originalStreamId);

            final Set<EventTransformation> eventTransformations = getEventTransformations(pass);
            final Action action = transformationChecker.requiresTransformation(jsonEnvelopeList, originalStreamId, pass);

            if (action.isKeepBackup()) {
                cloneStream(originalStreamId);
            }

            if (action.isTransform()) {
                final Optional<UUID> newStreamId = eventTransformationStreamIdFilter.getEventTransformationStreamId(eventTransformations, jsonEnvelopeList);
                if (newStreamId.isPresent()) {
                    streamMover.transformAndMoveStream(originalStreamId, eventTransformations, newStreamId.get());
                } else {
                    streamTransformer.transformStream(originalStreamId, eventTransformations);
                }
            }

            if (action.isDeactivate()) {
                streamRepository.deactivateStream(originalStreamId);
            }

        }catch (final Exception e){
            logger.error(format("Unknown error while moving events on stream %s", originalStreamId), e);
        }
        return originalStreamId;
    }

    @SuppressWarnings({"squid:S2629"})
    private void cloneStream(final UUID originalStreamId) {
        try {
            final UUID clonedStreamId = eventSource.cloneStream(originalStreamId);
            logger.info(format("Created backup stream '%s' from stream '%s'", clonedStreamId, originalStreamId));
        } catch (final EventStreamException e) {
            logger.error(format("Failed to backup stream %s", originalStreamId), e);
        }
    }

    private Set<EventTransformation> getEventTransformations(final int pass) {
        return eventTransformationRegistry.getEventTransformationBy(pass);
    }
}
