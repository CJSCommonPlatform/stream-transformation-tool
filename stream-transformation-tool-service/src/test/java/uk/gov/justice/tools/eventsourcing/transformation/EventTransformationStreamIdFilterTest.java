package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.services.messaging.Envelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static javax.json.Json.createObjectBuilder;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventTransformationStreamIdFilterTest {

    final static UUID STREAM_ID = UUID.randomUUID();

    @Test
    public void shouldReturnOptionalStreamId() throws Exception {

        TestTransformation testTransformation = new TestTransformation();
        TestTransformation1 testTransformation1 = new TestTransformation1();

        final JsonEnvelope event = buildEnvelope("test.me");
        final JsonEnvelope event1 = buildEnvelope("do.not.test.me");

        EventTransformationStreamIdFilter streamIdTransformer  = new EventTransformationStreamIdFilter();

        final Optional<UUID> streamId = streamIdTransformer.getEventTransformationStreamId(Sets.newHashSet(testTransformation, testTransformation1),
                asList(event, event1));

        assertTrue(streamId.isPresent());

        assertThat(streamId.get(), is(STREAM_ID));
    }

    @Test
    public void shouldReturnEmptyOptional(){

        TestTransformation testTransformation = new TestTransformation();
        TestTransformation1 testTransformation1 = new TestTransformation1();

        final JsonEnvelope event = buildEnvelope("no.stream.id");
        final JsonEnvelope event1 = buildEnvelope("no.stream.id2");

        EventTransformationStreamIdFilter streamIdTransformer  = new EventTransformationStreamIdFilter();

        final Optional<UUID> streamId = streamIdTransformer.getEventTransformationStreamId(Sets.newHashSet(testTransformation, testTransformation1),
                asList(event, event1));

        assertFalse(streamId.isPresent());
    }


    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }


    @Transformation
    public static class TestTransformation implements EventTransformation {

        @Override
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {
            return Stream.of(event);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            // Do nothing
        }

        public Optional<UUID> streamId(final JsonEnvelope event) {
            return Optional.empty();
        }
    }

    @Transformation
    public class TestTransformation1 implements EventTransformation {

        @Override
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {
            return Stream.of(event);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            // Do nothing
        }

        @Override
        public Optional<UUID> streamId(final JsonEnvelope event) {

            if (event.metadata().name().equals("test.me")) {
                return Optional.of(STREAM_ID);
            }

            return Optional.empty();
        }
    }
}
