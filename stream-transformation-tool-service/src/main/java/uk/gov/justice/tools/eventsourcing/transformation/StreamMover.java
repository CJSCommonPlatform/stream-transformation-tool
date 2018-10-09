package uk.gov.justice.tools.eventsourcing.transformation;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class StreamMover {

/*
    @Inject
    private Logger logger;

    @Inject
    private Enveloper enveloper;

    @Inject
    private EventSource eventSource;

    @Inject
    private StreamRepository streamRepository;

    public Optional<UUID> moveAndBackupStream(final UUID originalStreamId, final Set<EventTransformation> transformations) {
        try {
            final StreamMoveFilter streamFilter = new StreamMoveFilter(transformations);

            final UUID clonedStreamId = eventSource.cloneStream(originalStreamId);

            final EventStream originalEventStream = eventSource.getStreamById(originalStreamId);
            final List<JsonEnvelope> jsonEnvelopeList = originalEventStream.read().collect(toList());

            eventSource.clearStream(originalStreamId);

            appendUnprocessedEventsToOriginalStream(originalStreamId, streamFilter, jsonEnvelopeList);

            appendFilteredMoveEventsToNewStream(streamFilter, jsonEnvelopeList);

            return Optional.of(clonedStreamId);

        } catch (final EventStreamException e) {
            logger.error(format("Failed to backup stream %s", originalStreamId), e);
        } catch (final Exception e) {
            logger.error(format("Unknown error while moving events on stream %s", originalStreamId), e);
        }
        return empty();
    }

    private void appendFilteredMoveEventsToNewStream(final StreamMoveFilter streamFilter,
                                                     final List<JsonEnvelope> jsonEnvelopeList) throws EventStreamException {
        final UUID movedStreamId = streamRepository.createStream();
        final Stream<JsonEnvelope> filteredMoveEventStream = streamFilter.filterMoveEvents(jsonEnvelopeList);
        final EventStream newEventStream = eventSource.getStreamById(movedStreamId);
        newEventStream.append(filteredMoveEventStream.map(this::clearEventPositioning));
    }


    private void appendUnprocessedEventsToOriginalStream(final UUID originalStreamId,
                                                         final StreamMoveFilter streamMoveFilter,
                                                         final List<JsonEnvelope> jsonEnvelopeList) throws EventStreamException {

        final Stream<JsonEnvelope> unfilteredMoveEvents = streamMoveFilter.filterOriginalEvents(jsonEnvelopeList);

        final EventStream originalEventStream = eventSource.getStreamById(originalStreamId);
        originalEventStream.append(unfilteredMoveEvents.map(this::clearEventPositioning));
    }

    private JsonEnvelope clearEventPositioning(final JsonEnvelope event) {
        return enveloper.withMetadataFrom(event, event.metadata().name()).apply(event.payload());
    }
*/

}