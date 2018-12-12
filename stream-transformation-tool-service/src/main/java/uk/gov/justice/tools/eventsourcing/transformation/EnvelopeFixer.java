package uk.gov.justice.tools.eventsourcing.transformation;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;

public class EnvelopeFixer {

    @Inject
    private Enveloper enveloper;

    public JsonEnvelope clearPositionAndGiveNewId(final JsonEnvelope event) {
        return enveloper.withMetadataFrom(event, event.metadata().name()).apply(event.payload());
    }
}
