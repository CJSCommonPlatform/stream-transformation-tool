package uk.gov.sample.event.transformation;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation
public class SampleTransformation implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public boolean isApplicable(JsonEnvelope event) {
        return event.metadata().name().equalsIgnoreCase("people.events.person-gender-updated");
    }

    @Override
    public Stream<JsonEnvelope> apply(JsonEnvelope event) {
        final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, "sample.events.transformedName").apply(event.payload());
        return Stream.of(transformedEnvelope);
    }

    @Override
    public void setEnveloper(Enveloper enveloper) {
        this.enveloper = enveloper;
    }
}
