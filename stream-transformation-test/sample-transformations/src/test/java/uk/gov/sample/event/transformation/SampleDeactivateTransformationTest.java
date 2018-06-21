package uk.gov.sample.event.transformation;

import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.DEACTIVATE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import org.junit.Test;

public class SampleDeactivateTransformationTest {

    private SampleDeactivateTransformation underTest = new SampleDeactivateTransformation();

    @Test
    public void shouldCreateInstanceOfEventTransformation() {
        assertThat(underTest, instanceOf(EventTransformation.class));
    }

    @Test
    public void shouldSetDeactivateAction() {
        final JsonEnvelope event = buildEnvelope("sample.deactivate.events.name");

        assertThat(underTest.actionFor(event), is(DEACTIVATE));
    }

    @Test
    public void shouldSetNoAction() {
        final JsonEnvelope event = buildEnvelope("dummy.deactivate.events.name");

        assertThat(underTest.actionFor(event), is(NO_ACTION));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }

}
