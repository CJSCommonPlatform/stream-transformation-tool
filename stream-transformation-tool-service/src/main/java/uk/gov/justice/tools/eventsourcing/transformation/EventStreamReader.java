package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class EventStreamReader{

    @Inject
    private EventSource eventSource;

    public List<JsonEnvelope> getStreamBy(final UUID streamId) {
        try(final Stream<JsonEnvelope> envelopeStream = eventSource.getStreamById(streamId).read()) {
            final List<JsonEnvelope> jsonEnvelopeList = envelopeStream.collect(toList());

            return jsonEnvelopeList;
        }
    }
}
