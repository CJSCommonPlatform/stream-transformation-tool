package uk.gov.justice.tools.eventsourcing.transformation;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;

public class EventTransformationStreamIdFilterTest {

    private static final String SOURCE_EVENT_NAME = "eventSourceName";
    private static final String SOURCE_EVENT_NAME_2 = "eventSourceName2";
    private static final UUID STREAM_ID = randomUUID();

    @Test
    public void shouldReturnStreamId() throws Exception {
        final JsonEnvelope event1 = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(SOURCE_EVENT_NAME_2);

        final TestTransformation1 transformation1 = new TestTransformation1();
        final TestTransformation1 transformation2 = new TestTransformation1();

        final EventTransformationStreamIdFilter eventTransformationStreamIdFilter = new EventTransformationStreamIdFilter();
        final Optional<UUID> eventTransformationStreamId = eventTransformationStreamIdFilter.getEventTransformationStreamId(newHashSet(transformation1, transformation2), Arrays.asList(event1, event2));

        assertTrue(eventTransformationStreamId.isPresent());
    }

    @Test
    public void shouldNotReturnStreamId() throws Exception {
        final JsonEnvelope event1 = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(SOURCE_EVENT_NAME_2);

        final TestTransformation transformation1 = new TestTransformation();

        final EventTransformationStreamIdFilter eventTransformationStreamIdFilter = new EventTransformationStreamIdFilter();
        final Optional<UUID> eventTransformationStreamId = eventTransformationStreamIdFilter.getEventTransformationStreamId(newHashSet(transformation1), Arrays.asList(event1, event2));

        assertFalse(eventTransformationStreamId.isPresent());
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


    }

    @Transformation
    public static class TestTransformation1 implements EventTransformation {

        @Override
        public Action actionFor(final JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase("eventSourceName") ||
                    event.metadata().name().equalsIgnoreCase("eventSourceName2")) {
                return TRANSFORM;
            }
            return NO_ACTION;
        }

        @Override
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {

            return Stream.of(event);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            // Do nothing
        }

        public Optional<UUID> setStreamId(final JsonEnvelope event) {
            return Optional.of(STREAM_ID);
        }
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
}