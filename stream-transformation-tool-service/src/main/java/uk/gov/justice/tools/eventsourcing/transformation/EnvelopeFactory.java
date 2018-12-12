package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.MetadataBuilder;

import java.util.UUID;

import javax.inject.Inject;
import javax.json.JsonValue;

public class EnvelopeFactory {

    private final ObjectToJsonValueConverter objectToJsonValueConverter;

    @Inject
    public EnvelopeFactory(final ObjectToJsonValueConverter objectToJsonValueConverter) {
        this.objectToJsonValueConverter = objectToJsonValueConverter;
    }

    public JsonEnvelope createFrom(final Metadata metadata, final Object payload) {
        return createFrom(metadata, metadata.name(), payload);
    }

    public JsonEnvelope createFrom(final Metadata metadata, final String name, final Object payload) {
        final JsonValue payloadAsJson = objectToJsonValueConverter.convert(payload);

        final MetadataBuilder metadataBuilder = metadataBuilder()
                .withId(randomUUID())
                .withName(name);

        metadata.clientCorrelationId().ifPresent(metadataBuilder::withClientCorrelationId);
        metadata.sessionId().ifPresent(metadataBuilder::withSessionId);
        metadata.userId().ifPresent(metadataBuilder::withUserId);
        metadata.streamId().ifPresent(metadataBuilder::withStreamId);
        metadata.createdAt().ifPresent(metadataBuilder::createdAt);

        if (! metadata.causation().isEmpty()) {
            metadataBuilder.withCausation(metadata.causation().toArray(new UUID[0]));
        }

        return envelopeFrom(metadataBuilder, payloadAsJson);
    }
}
