package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.Optional.empty;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.StreamTransformerUtil;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

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
    private StreamTransformerUtil streamTransformerUtil;

    @SuppressWarnings({"squid:S2629"})
    public Optional<UUID> transformAndBackupStream(final UUID streamId, final Set<EventTransformation> transformations) {

        try {
            final UUID backupStreamId = eventSource.cloneStream(streamId);

            logger.info(format("created backup stream '%s' from stream '%s'", backupStreamId, streamId));

            final EventStream stream = eventSource.getStreamById(streamId);
            final Stream<JsonEnvelope> events = stream.read();

            eventSource.clearStream(streamId);

            logger.info("transforming events on stream {}", streamId);

            final Stream<JsonEnvelope> transformedEventStream = streamTransformerUtil.transform(events, transformations);

            stream.append(transformedEventStream.map(this::clearEventPositioning));

            events.close();

            return Optional.of(backupStreamId);

        } catch (final EventStreamException e) {
            logger.error(format("Failed to backup stream %s", streamId), e);
        } catch (final Exception e) {
            logger.error(format("Unknown error while transforming events on stream %s", streamId), e);
        }
        return empty();
    }

    private JsonEnvelope clearEventPositioning(final JsonEnvelope event) {
        return enveloper.withMetadataFrom(event, event.metadata().name()).apply(event.payload());
    }

}
