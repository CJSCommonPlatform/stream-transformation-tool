package uk.gov.sample.event.transformation;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

public class SampleTransformationTest {

    private static final String SOURCE_EVENT_NAME = "sample.events.name";
    private static final String TRANSFORMED_EVENT_NAME = "sample.events.transformedName";

    private SampleTransformation sampleTransformation = new SampleTransformation();

    private Enveloper enveloper = createEnveloper();

    @Before
    public void setup() {
        sampleTransformation.setEnveloper(enveloper);
    }

    @Test
    public void shouldCreateInstanceOfEventTransformation() {
        assertThat(sampleTransformation, instanceOf(EventTransformation.class));
    }

    @Test
    public void shouldSetIsApplicable() {
        final JsonEnvelope event = buildEnvelope("sample.events.name");
        
        assertTrue(sampleTransformation.isApplicable(event));
    }
    
    @Test
    public void shouldCreateTransformation() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        
        final Stream<JsonEnvelope> transformedStream = sampleTransformation.apply(event);

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
