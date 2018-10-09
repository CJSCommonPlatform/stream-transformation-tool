package uk.gov.sample.event.transformation.move;

import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation(pass = 6)
public class SampleTransformationMoveWithoutBackUp implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public Action actionFor(final JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.transformation.move.without.backup")) {
            return new Action(true, false, false);
        }
        return NO_ACTION;
    }

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, "sample.transformation.move.without.backup.transformed").apply(event.payload());
        return Stream.of(transformedEnvelope);
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {
        this.enveloper = enveloper;
    }

}
