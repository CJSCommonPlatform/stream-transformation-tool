package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataFrom;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.MetadataBuilder;

import java.util.UUID;

public class NewEnvelopeCreator {

    public JsonEnvelope toNewEnvelope(final JsonEnvelope event, final UUID streamId, final long version) {

        final Metadata metadata = event.metadata();
        final MetadataBuilder metadataBuilder = metadataFrom(metadata)
                .withId(randomUUID())
                .withStreamId(streamId)
                .withVersion(version);

        metadata.createdAt().ifPresent(metadataBuilder::createdAt);

        return envelopeFrom(metadataBuilder, event.payloadAsJsonObject());

    }
}
