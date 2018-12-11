package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.MetadataBuilder;

import javax.json.JsonValue;

public class EventPositionClearer {

    public JsonEnvelope clearEventPositioning(final JsonEnvelope event) {

        final Metadata metadata = event.metadata();
        final JsonValue payload = event.payload();

        final MetadataBuilder metadataBuilder = metadataBuilder()
                .withId(randomUUID())
                .withName(metadata.name());

        metadata.clientCorrelationId().ifPresent(metadataBuilder::withClientCorrelationId);
        metadata.sessionId().ifPresent(metadataBuilder::withSessionId);
        metadata.userId().ifPresent(metadataBuilder::withUserId);
        metadata.streamId().ifPresent(metadataBuilder::withStreamId);
        metadata.createdAt().ifPresent(metadataBuilder::createdAt);

        return envelopeFrom(metadataBuilder, payload);
    }
}
