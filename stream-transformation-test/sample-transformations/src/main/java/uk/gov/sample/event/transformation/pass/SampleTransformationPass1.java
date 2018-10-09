package uk.gov.sample.event.transformation.pass;

import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation(pass = 4)
public class SampleTransformationPass1 implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public Action actionFor(final JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.events.name.pass")) {
            return TRANSFORM;
        }
        return NO_ACTION;
    }

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, "sample.events.transformedName.pass").apply(event.payload());
        return Stream.of(transformedEnvelope);
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {
        this.enveloper = enveloper;
    }
}
