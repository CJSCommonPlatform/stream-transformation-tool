package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.source.core.EventSourceTransformation;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventStreamReader;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationStreamIdFilter;
import uk.gov.justice.tools.eventsourcing.transformation.TransformationChecker;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
    private EventSourceTransformation eventSourceTransformation;

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

    @Inject
    private RetryStreamOperator retryStreamOperator;


    @Transactional(value = REQUIRES_NEW, rollbackOn = {EventStreamException.class, JdbcRepositoryException.class})
    public UUID transformEventStream(final UUID originalStreamId, final int pass) throws EventStreamException {

        try {
            final List<JsonEnvelope> jsonEnvelopeList = eventStreamReader.getStreamBy(originalStreamId);

            final Set<EventTransformation> eventTransformations = getEventTransformations(pass);
            final Action action = transformationChecker.requiresTransformation(jsonEnvelopeList, originalStreamId, pass);

            if (action.isKeepBackup()) {
                retryStreamOperator.cloneWithRetry(originalStreamId);
            }

            if (action.isTransform()) {
                final Optional<UUID> newStreamId = eventTransformationStreamIdFilter.getEventTransformationStreamId(eventTransformations, jsonEnvelopeList);
                if (isNewStreamId(newStreamId, originalStreamId)) {
                    streamMover.transformAndMoveStream(originalStreamId, eventTransformations, newStreamId.get());
                } else {
                    streamTransformer.transformStream(originalStreamId, eventTransformations);
                }
            }

            if (action.isDeactivate()) {
                streamRepository.deactivateStream(originalStreamId);
            }
        } catch (final EventStreamException | JdbcRepositoryException e) {
            logger.error(format("Unknown error while transforming events on stream %s", originalStreamId), e);
            throw e;
        }

        return originalStreamId;
    }

    private boolean isNewStreamId(final Optional<UUID> newStreamId,
                                  final UUID originalStreamId) {
        return newStreamId.isPresent() && !newStreamId.get().equals(originalStreamId);
    }

    private Set<EventTransformation> getEventTransformations(final int pass) {
        return eventTransformationRegistry.getEventTransformationBy(pass);
    }
}
