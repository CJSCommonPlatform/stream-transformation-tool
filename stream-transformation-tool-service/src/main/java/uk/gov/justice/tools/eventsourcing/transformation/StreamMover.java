package uk.gov.justice.tools.eventsourcing.transformation;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class StreamMover {

    @Inject
    private Logger logger;


    @Inject
    private EventSource eventSource;

    @Inject
    private StreamAppender streamRepository;

    @Inject
    private StreamTransformerUtil streamTransformerUtil;

    public void transformAndMoveStream(final UUID originalStreamId,
                                       final Set<EventTransformation> transformations,
                                       final UUID newStreamId) {
        try {
            final EventStream originalEventStream = eventSource.getStreamById(originalStreamId);
            final List<JsonEnvelope> jsonEnvelopeList = originalEventStream.read().collect(toList());

            eventSource.clearStream(originalStreamId);

            final Stream<JsonEnvelope> filteredMoveEventStream = streamTransformerUtil.transformAndMove(jsonEnvelopeList.stream(), transformations);
            final Stream<JsonEnvelope> unfilteredMoveEventStream = streamTransformerUtil.filterOriginalEvents(jsonEnvelopeList, transformations);

            streamRepository.appendEventsToStream(originalStreamId, unfilteredMoveEventStream);
            streamRepository.appendEventsToStream(newStreamId, filteredMoveEventStream);

        } catch (final Exception e) {
            logger.error(format("Unknown error while moving events on stream %s", originalStreamId), e);
        }
    }


}