package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
import uk.gov.justice.tools.eventsourcing.transformation.TransformationChecker;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

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
    private StreamRepository streamRepository;

    @Inject
    private EventTransformationRegistry eventTransformationRegistry;

    @Inject
    private TransformationChecker transformationChecker;


    @Transactional(REQUIRES_NEW)
    public UUID transformEventStream(final UUID originalStreamId, final int pass) {
        final Stream<JsonEnvelope> eventStream = eventSource.getStreamById(originalStreamId).read();

        final Set<EventTransformation> eventTransformations = getEventTransformations(pass);

        final Action action = transformationChecker.requiresTransformation(eventStream, originalStreamId, pass);

        //TODO refactor cloning
        final Optional<UUID> backupStreamId = streamTransformer.transformAndBackupStream(originalStreamId, eventTransformations);
        deleteBackUpstreamIfNeeded(backupStreamId, originalStreamId, action);

        if (action.isDeactivate()) {
            streamRepository.deactivateStream(originalStreamId);
        }

        eventStream.close();
        return originalStreamId;
    }

    @SuppressWarnings({"squid:S2629"})
    private void deleteBackUpstreamIfNeeded(final Optional<UUID> backupStreamId,
                                            final UUID originalStreamId,
                                            final Action action) {
        if (!action.isKeepBackup()) {
            if (backupStreamId.isPresent()) {
                streamRepository.deleteStream(backupStreamId.get());
            } else {
                logger.info(format("Cannot delete backup stream. No backup stream was created for stream '%s'", originalStreamId));
            }
        }
    }

    private Set<EventTransformation> getEventTransformations(final int pass) {
        return eventTransformationRegistry.getEventTransformationBy(pass);
    }
}
