package uk.gov.sample.event.transformation;

import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.ARCHIVE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.TRANSFORM_EVENT;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation
public class SampleTransformationV2 implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public TransformAction action(JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.v2.events.name")){
            return TRANSFORM_EVENT;
        }
        return NO_ACTION;
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
