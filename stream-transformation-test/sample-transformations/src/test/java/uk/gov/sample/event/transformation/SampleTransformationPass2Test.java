package uk.gov.sample.event.transformation;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

public class SampleTransformationPass2Test {

    private static final String SOURCE_EVENT_NAME = "sample.events.transformedName.pass";
    private static final String TRANSFORMED_EVENT_NAME = "sample.events.transformedName.pass2";

    private SampleTransformationPass2 sampleTransformationPass2 = new SampleTransformationPass2();

    private Enveloper enveloper = createEnveloper();

    @Before
    public void setup() {
        sampleTransformationPass2.setEnveloper(enveloper);
    }

    @Test
    public void shouldCreateInstanceOfEventTransformation() {
        assertThat(sampleTransformationPass2, instanceOf(EventTransformation.class));
    }

    @Test
    public void shouldSetTransformAction() {
        final JsonEnvelope event = buildEnvelope("sample.events.transformedName.pass");

        assertThat(sampleTransformationPass2.actionFor(event), is(TRANSFORM));
    }

    @Test
    public void shouldSetNoAction() {
        final JsonEnvelope event = buildEnvelope("dummy.sample.events.transformedName.pass");

        assertThat(sampleTransformationPass2.actionFor(event), is(NO_ACTION));
    }
    
    @Test
    public void shouldCreateTransformation() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        
        final Stream<JsonEnvelope> transformedStream = sampleTransformationPass2.apply(event);

        final List<JsonEnvelope> transformedEvents = transformedStream.collect(toList());
        assertThat(transformedEvents, hasSize(1));
        assertThat(transformedEvents.get(0).metadata().name(), is(TRANSFORMED_EVENT_NAME));

        assertThat(transformedEvents.get(0).payloadAsJsonObject().getString("field"),
                is(event.payloadAsJsonObject().getString("field")));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
    
}
