package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

public class StreamTransformerUtilTest {

    final UUID STREAM_ID = randomUUID();

    private static final String EVENT_NAME = "sample.events.name";
    private static final String EVENT_NAME_PASS = "sample.events.name.pass";
    private static final String TRANSFORM_NAME = "sample.events.transformedName";

    @Test
    public void shouldTestTransform() {

        final StreamTransformerUtil streamTransformerUtil = new StreamTransformerUtil();

        final JsonEnvelope event = buildEnvelope(EVENT_NAME);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event);
        final TestTransformation transformation1 = new TestTransformation();
        final Set<EventTransformation> transformations = new HashSet<>();
        transformations.add(transformation1);

        final Stream<JsonEnvelope> transformedJsonEnvelopeStream = streamTransformerUtil.transform(jsonEnvelopeStream, transformations);

        final List<JsonEnvelope> transformedJsonEnvelopeList = transformedJsonEnvelopeStream.collect(Collectors.toList());

        assertThat(transformedJsonEnvelopeList.size(), is(1));

        assertThat(transformedJsonEnvelopeList.get(0).metadata().name(), is(TRANSFORM_NAME));
    }

    @Test
    public void shouldTestNoTransform() {

        final StreamTransformerUtil streamTransformerUtil = new StreamTransformerUtil();

        final JsonEnvelope event = buildEnvelope(EVENT_NAME_PASS);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event);
        final TestTransformation transformation1 = new TestTransformation();
        final Set<EventTransformation> transformations = new HashSet<>();
        transformations.add(transformation1);

        final Stream<JsonEnvelope> transformedJsonEnvelopeStream = streamTransformerUtil.transform(jsonEnvelopeStream, transformations);

        final List<JsonEnvelope> transformedJsonEnvelopeList = transformedJsonEnvelopeStream.collect(Collectors.toList());

        assertThat(transformedJsonEnvelopeList.size(), is(1));

        assertThat(transformedJsonEnvelopeList.get(0).metadata().name(), is(EVENT_NAME_PASS));
    }

    @Transformation
    public static class TestTransformation implements EventTransformation {

        private Enveloper enveloper = createEnveloper();

        @Override
        public Action actionFor(final JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase(EVENT_NAME)) {
                return TRANSFORM;
            }
            return NO_ACTION;
        }

        @Override
        public Stream<JsonEnvelope> apply(final JsonEnvelope event) {

            final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, TRANSFORM_NAME).apply(event.payload());
            return Stream.of(transformedEnvelope);
        }

        @Override
        public void setEnveloper(final Enveloper enveloper) {
            this.enveloper = enveloper;
        }
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
}
