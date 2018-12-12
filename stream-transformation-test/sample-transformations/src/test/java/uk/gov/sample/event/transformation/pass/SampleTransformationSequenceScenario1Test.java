package uk.gov.sample.event.transformation.pass;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SampleTransformationSequenceScenario1Test {

    private static final String SOURCE_EVENT_NAME = "sample.events.name.pass1.sequence";
    private static final String TRANSFORMED_EVENT_NAME_1 = "sample.events.name.pass1.sequence1";
    private static final String TRANSFORMED_EVENT_NAME_2 = "sample.events.name.pass1.sequence2";
    private static final String TRANSFORMED_EVENT_NAME_3 = "sample.events.name.pass1.sequence3";

    private SampleTransformationSequenceScenario1 sampleTransformationSequenceScenario1 = new SampleTransformationSequenceScenario1();

    private Enveloper enveloper = createEnveloper();

    @Before
    public void setup() {
        sampleTransformationSequenceScenario1.setEnveloper(enveloper);
    }

    @Test
    public void shouldCreateInstanceOfEventTransformation() {
        assertThat(sampleTransformationSequenceScenario1, instanceOf(EventTransformation.class));
    }

    @Test
    public void shouldSetTransformAction() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        assertThat(sampleTransformationSequenceScenario1.actionFor(event), is(new Action(true,false,false)));
    }

    @Test
    public void shouldSetNoAction() {
        final JsonEnvelope event = buildEnvelope(TRANSFORMED_EVENT_NAME_1);

        assertThat(sampleTransformationSequenceScenario1.actionFor(event), is(NO_ACTION));
    }

    @Test
    public void shouldCreateTransformation() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        final Stream<JsonEnvelope> transformedStream = sampleTransformationSequenceScenario1.apply(event);

        final List<JsonEnvelope> transformedEvents = transformedStream.collect(toList());
        assertThat(transformedEvents, hasSize(3));
        assertThat(transformedEvents.get(0).metadata().name(), is(TRANSFORMED_EVENT_NAME_1));
        assertThat(transformedEvents.get(1).metadata().name(), is(TRANSFORMED_EVENT_NAME_2));
        assertThat(transformedEvents.get(2).metadata().name(), is(TRANSFORMED_EVENT_NAME_3));

        assertThat(transformedEvents.get(0).payloadAsJsonObject().getString("field"),
                is(event.payloadAsJsonObject().getString("field")));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
}
