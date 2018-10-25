package uk.gov.sample.event.transformation.transform;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;


public class SampleTransformationWithStreamIdTest {

    private static final String SOURCE_EVENT_NAME = "sample.transformation.with.stream.id";
    private static final String TRANSFORMED_EVENT_NAME = "sample.transformation.with.stream.id.transformed";

    private SampleTransformationWithStreamId sampleTransformationWithStreamId = new SampleTransformationWithStreamId();

    private Enveloper enveloper = createEnveloper();

    @Before
    public void setup() {
        sampleTransformationWithStreamId.setEnveloper(enveloper);
    }

    @Test
    public void shouldCreateInstanceOfEventTransformation() {
        assertThat(sampleTransformationWithStreamId, instanceOf(EventTransformation.class));
    }

    @Test
    public void shouldSetTransformAction() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        assertThat(sampleTransformationWithStreamId.actionFor(event), is(TRANSFORM));
    }

    @Test
    public void shouldSetNoAction() {
        final JsonEnvelope event = buildEnvelope(TRANSFORMED_EVENT_NAME);

        assertThat(sampleTransformationWithStreamId.actionFor(event), is(NO_ACTION));
    }

    @Test
    public void shouldCreateTransformation() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        final Stream<JsonEnvelope> transformedStream = sampleTransformationWithStreamId.apply(event);

        final List<JsonEnvelope> transformedEvents = transformedStream.collect(toList());
        assertThat(transformedEvents, hasSize(1));
        assertThat(transformedEvents.get(0).metadata().name(), is(TRANSFORMED_EVENT_NAME));

        assertThat(transformedEvents.get(0).payloadAsJsonObject().getString("field"),
                is(event.payloadAsJsonObject().getString("field")));
    }

    @Test
    public void shouldReturnStreamId() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Optional<UUID> streamId = sampleTransformationWithStreamId.setStreamId(event);
        assertTrue(streamId.isPresent());
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }

}