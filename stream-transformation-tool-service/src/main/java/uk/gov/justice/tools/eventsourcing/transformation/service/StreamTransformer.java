package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
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

    @SuppressWarnings({"squid:S2629"})
    public Optional<UUID> transformAndBackupStream(final UUID originalStreamId, final Set<EventTransformation> transformations) {
        try {
            final StreamTransformerUtil streamTransformerUtil = new StreamTransformerUtil(transformations);

            final EventStream eventStream = eventSource.getStreamById(originalStreamId);
            final List<JsonEnvelope> originalEventList = eventStream
                    .read()
                    .collect(toList());

            final UUID backupStreamId = eventSource.cloneStream(originalStreamId);
            eventSource.clearStream(originalStreamId);

            logger.info(format("Created backup originalEventStream '%s' from originalEventStream '%s'", backupStreamId, originalStreamId));
            logger.info("Transforming events on stream {}", originalStreamId);

            final List<JsonEnvelope> transformedEventStream = streamTransformerUtil.transform(originalEventList, transformations);
            final Optional<UUID> eventTransformationStreamId = eventTransformationStreamIdFilter.getEventTransformationStreamId(transformations, originalEventList);

            if (eventTransformationStreamId.isPresent()) {
                logger.info(format("Move EventStream id  %s ", eventTransformationStreamId.get()));

                appendFilteredEventsToStream(eventTransformationStreamId.get(), transformedEventStream);
                appendUnprocessedEventsToOriginalStream(originalStreamId, originalEventList, streamTransformerUtil);

            } else {
                final List<JsonEnvelope> transformedEventStream1 = streamTransformerUtil.getTransformedEvents(originalEventList, transformations);
                appendToStream(originalStreamId, transformedEventStream1);
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

        streamRepository.createStreamIfNeeded(streamId);//
        appendToStream(streamId, filteredEventStream);
    }

    private void appendUnprocessedEventsToOriginalStream(final UUID originalStreamId,
                                                         final List<JsonEnvelope> jsonEnvelopeList,
                                                         final StreamTransformerUtil streamTransformerUtil) throws EventStreamException {

        final List<JsonEnvelope> unfilteredMoveEvents = streamTransformerUtil.filterOriginalEvents(jsonEnvelopeList.stream());

        appendToStream(originalStreamId, unfilteredMoveEvents);
    }

    private void appendToStream(final UUID streamId, final List<JsonEnvelope> jsonEnvelopeList) throws EventStreamException {
        final EventStream eventStream = eventSource.getStreamById(streamId);
        eventStream.append(jsonEnvelopeList.stream().map(this::clearEventPositioning));
    }
}
