package uk.gov.sample.event.transformation.pass;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
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
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

@RunWith(MockitoJUnitRunner.class)
public class SampleTransformationSequenceScenario2Test {

    private static final String SOURCE_EVENT_NAME = "sample.events.name.pass1.sequence2";


    private SampleTransformationSequenceScenario2 sampleTransformationSequenceScenario2 = new SampleTransformationSequenceScenario2();

    private Enveloper enveloper = createEnveloper();

    @Before
    public void setup() {
        sampleTransformationSequenceScenario2.setEnveloper(enveloper);
    }

    @Test
    public void shouldCreateInstanceOfEventTransformation() {
        assertThat(sampleTransformationSequenceScenario2, instanceOf(EventTransformation.class));
    }

    @Test
    public void shouldSetTransformAction() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        assertThat(sampleTransformationSequenceScenario2.actionFor(event), is(new Action(true,false,false)));
    }

    @Test
    public void shouldSetNoAction() {
        final JsonEnvelope event = buildEnvelope("should.not.transform");

        assertThat(sampleTransformationSequenceScenario2.actionFor(event), is(NO_ACTION));
    }

    @Test
    public void shouldCreateTransformation() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        final Stream<JsonEnvelope> transformedStream = sampleTransformationSequenceScenario2.apply(event);

        final List<JsonEnvelope> transformedEvents = transformedStream.collect(toList());
        assertThat(transformedEvents, hasSize(1));
        assertThat(transformedEvents.get(0).metadata().name(), is(SOURCE_EVENT_NAME));


        assertThat(transformedEvents.get(0).payloadAsJsonObject().getString("field"),
                is(event.payloadAsJsonObject().getString("field")));
    }

    @Test
    public void shouldReturnStreamId() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Optional<UUID> streamId = sampleTransformationSequenceScenario2.setStreamId(event);
        assertTrue(streamId.isPresent());
    }


    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
}