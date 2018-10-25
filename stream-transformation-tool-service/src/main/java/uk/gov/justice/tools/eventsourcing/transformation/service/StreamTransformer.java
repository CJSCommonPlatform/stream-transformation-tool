package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.StreamAppender;
import uk.gov.justice.tools.eventsourcing.transformation.StreamTransformerUtil;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

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
    private StreamAppender streamAppender;

    @Inject
    private StreamTransformerUtil streamTransformerUtil;

    @SuppressWarnings({"squid:S2629"})
    public void transformStream(final UUID streamId, final Set<EventTransformation> transformations) {
        try {
            final EventStream stream = eventSource.getStreamById(streamId);
            final Stream<JsonEnvelope> events = stream.read();

            eventSource.clearStream(streamId);

            logger.info("Transforming events on stream {}", streamId);

            final Stream<JsonEnvelope> transformedEventStream = streamTransformerUtil.transform(events, transformations);

            streamAppender.appendEventsToStream(streamId , transformedEventStream);

            events.close();

        } catch (final Exception e) {
            logger.error(format("Unknown error while transforming events on stream %s", streamId), e);
        }

    }

}
