package uk.gov.sample.event.transformation.anonymise;

import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.anonymization.EventAnonymiserTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

@Transformation
public class SampleAnonymisationTransformation extends EventAnonymiserTransformation {
    private Enveloper enveloper;

    @Override
    public Action actionFor(final JsonEnvelope jsonEnvelope) {
        if (jsonEnvelope.metadata().name().equalsIgnoreCase("sample.transformation.anonymise")) {
            return super.actionFor(jsonEnvelope);
        }
        return NO_ACTION;
    }

}
