package uk.gov.justice.tools.eventsourcing;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EnvelopeFactory;

import java.util.function.Function;

public class TransformingEnveloper implements Enveloper {

    private final EnvelopeFactory envelopeFactory;

    public TransformingEnveloper(final EnvelopeFactory envelopeFactory) {
        this.envelopeFactory = envelopeFactory;
    }

    @Override
    public Function<Object, JsonEnvelope> withMetadataFrom(final JsonEnvelope envelope) {
        return payload -> envelopeFactory.createFrom(envelope.metadata(), payload);
    }

    @Override
    public Function<Object, JsonEnvelope> withMetadataFrom(final JsonEnvelope envelope, final String name) {
        return payload -> envelopeFactory.createFrom(envelope.metadata(), name, payload);
    }
}
