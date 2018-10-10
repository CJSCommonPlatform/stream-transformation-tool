package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.Optional.*;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationStreamIdFilter;
import uk.gov.justice.tools.eventsourcing.transformation.StreamTransformerUtil;
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
public class StreamTransformer {

    @Inject
    private Logger logger;

    @Inject
    private EventSource eventSource;

    @Inject
    private Enveloper enveloper;

    @Inject
    private StreamRepository streamRepository;

    @Inject
    private EventTransformationStreamIdFilter eventTransformationStreamIdFilter;

    public StreamTransformer() {
    }

    @SuppressWarnings({"squid:S2629"})
    public Optional<UUID> transformAndBackupStream(final UUID originalStreamId, final Set<EventTransformation> transformations) {
        try {
            final StreamTransformerUtil streamTransformerUtil = new StreamTransformerUtil(transformations);

            final UUID backupStreamId = eventSource.cloneStream(originalStreamId);
            eventSource.clearStream(originalStreamId);

            logger.info(format("Created backup originalEventStream '%s' from originalEventStream '%s'", backupStreamId, originalStreamId));

            final EventStream eventStream = eventSource.getStreamById(originalStreamId);
            final List<JsonEnvelope> originalEventList = eventStream.read().collect(toList());

            logger.info("Transforming events on stream {}", originalStreamId);

            final List<JsonEnvelope> transformedEventStream = streamTransformerUtil.transform(originalEventList, transformations);

            final Optional<UUID> eventTransformationStreamId = eventTransformationStreamIdFilter.getEventTransformationStreamId(transformations, transformedEventStream);

            if(eventTransformationStreamId.isPresent()){
                logger.info(String.format("move originalEventStream id is %s ", eventTransformationStreamId.get()));

                appendFilteredEventsToStream(eventTransformationStreamId.get(), transformedEventStream);
                appendUnprocessedEventsToOriginalStream(originalStreamId, originalEventList, streamTransformerUtil);
            }
            else{
                eventStream.append(transformedEventStream.stream().map(this::clearEventPositioning));
            }

            return of(backupStreamId);

        } catch (final EventStreamException e) {
            logger.error(format("Failed to backup stream %s", originalStreamId), e);
        } catch (final Exception e) {
            logger.error(format("Unknown error while transforming events on stream %s", originalStreamId), e);
        }
        return empty();
    }

    private JsonEnvelope clearEventPositioning(final JsonEnvelope event) {
        return enveloper.withMetadataFrom(event, event.metadata().name()).apply(event.payload());
    }

    private void appendFilteredEventsToStream(final UUID streamId,
                                              final List<JsonEnvelope> filteredEventStream) throws EventStreamException {

        final UUID moveStreamId = streamRepository.createStreamIfNeeded(streamId);
        appendStream(moveStreamId, filteredEventStream);
    }

    private void appendUnprocessedEventsToOriginalStream(final UUID originalStreamId,
                                                         final List<JsonEnvelope> jsonEnvelopeList,
                                                         final StreamTransformerUtil streamTransformerUtil) throws EventStreamException {

        final List<JsonEnvelope> unfilteredMoveEvents = streamTransformerUtil.filterOriginalEvents(jsonEnvelopeList.stream());

        appendStream(originalStreamId, unfilteredMoveEvents);
    }

    private void appendStream(final UUID streamId, final List<JsonEnvelope> eventStream) throws EventStreamException {
        final EventStream originalEventStream = eventSource.getStreamById(streamId);
        originalEventStream.append(eventStream.stream().map(this::clearEventPositioning));
    }
}
