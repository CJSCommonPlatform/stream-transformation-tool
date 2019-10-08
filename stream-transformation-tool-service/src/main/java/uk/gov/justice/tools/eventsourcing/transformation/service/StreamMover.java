package uk.gov.justice.tools.eventsourcing.transformation.service;

import uk.gov.justice.services.eventsourcing.source.core.EventSourceTransformation;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventStreamReader;
import uk.gov.justice.tools.eventsourcing.transformation.StreamTransformerUtil;
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
    private EventSourceTransformation eventSourceTransformation;

    @Inject
    private StreamAppender streamAppender;

    @Inject
    private StreamTransformerUtil streamTransformerUtil;

    @Inject
    private EventStreamReader eventStreamReader;

    public void transformAndMoveStream(final UUID originalStreamId,
                                       final Set<EventTransformation> transformations,
                                       final UUID newStreamId) throws EventStreamException {
        final List<JsonEnvelope> jsonEnvelopeList = eventStreamReader.getStreamBy(originalStreamId);

        eventSourceTransformation.clearStream(originalStreamId);

        try (final Stream<JsonEnvelope> filteredMoveEventStream = streamTransformerUtil.transformAndMove(jsonEnvelopeList.stream(), transformations)) {
            streamAppender.appendEventsToStream(newStreamId, filteredMoveEventStream);
        }

        try (final Stream<JsonEnvelope> unfilteredMoveEventStream = streamTransformerUtil.filterOriginalEvents(jsonEnvelopeList, transformations)) {
            streamAppender.appendEventsToStream(originalStreamId, unfilteredMoveEventStream);
        }

    }


}