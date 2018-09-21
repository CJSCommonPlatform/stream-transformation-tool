package uk.gov.sample.event.transformation.transform;

import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.DEACTIVATE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;


@Transformation
public class SampleDeactivateTransformation implements EventTransformation {

    @Override
    public Action actionFor(final JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.deactivate.events.name")) {
            return DEACTIVATE;
        }
        return NO_ACTION;
    }

    @Override
    public void setEnveloper(Enveloper enveloper) {
        // Unused by test
    }

}
