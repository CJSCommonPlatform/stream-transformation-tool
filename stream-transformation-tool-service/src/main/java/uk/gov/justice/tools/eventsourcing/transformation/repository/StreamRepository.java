package uk.gov.justice.tools.eventsourcing.transformation.repository;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class StreamRepository {

    @Inject
    private Logger logger;

    @Inject
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @Inject
    private EventSource eventSource;

    @SuppressWarnings({"squid:S2629"})
    public void deleteStream(final UUID streamId) {
        eventStreamJdbcRepository.delete(streamId);
        logger.info(format("Deleted stream '%s'", streamId));
    }

    @SuppressWarnings({"squid:S2629"})
    public void deactivateStream(final UUID streamId) {
        eventStreamJdbcRepository.markActive(streamId, false);
        logger.info(format("deactivated/archived stream '%s'", streamId));
    }

    //TODO we don't need this
    public UUID createStream() {
        final UUID streamId = randomUUID();
        eventStreamJdbcRepository.insert(streamId);
        return streamId;
    }

    public void createStreamIfNeeded(final UUID streamId) {
        final Stream<JsonEnvelope> jsonEnvelopeStream = eventSource.getStreamById(streamId).read();
        final boolean isStreamEmpty = jsonEnvelopeStream.collect(toList()).isEmpty();
        if(isStreamEmpty){
            eventStreamJdbcRepository.insert(streamId);
        }
    }
}
