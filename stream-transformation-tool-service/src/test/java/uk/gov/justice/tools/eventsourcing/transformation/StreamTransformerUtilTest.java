package uk.gov.justice.tools.eventsourcing.transformation;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static junit.framework.TestCase.assertTrue;
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

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

public class StreamTransformerUtilTest {

    final UUID STREAM_ID = randomUUID();

    private static final String EVENT_NAME = "sample.events.name";
    private static final String EVENT_NAME_PASS = "sample.events.name.pass";
    private static final String TRANSFORM_NAME = "sample.events.transformedName";

    private StreamTransformerUtil streamTransformerUtil;

    @Before
    public void setup() {
        streamTransformerUtil = new StreamTransformerUtil();
    }

    @Test
    public void shouldTransformEvents() {

        final JsonEnvelope event = buildEnvelope(EVENT_NAME);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event);

        final Stream<JsonEnvelope> transformedJsonEnvelopeStream = streamTransformerUtil.transform(jsonEnvelopeStream, newHashSet(new TestTransformation()));

        final List<JsonEnvelope> transformedJsonEnvelopeList = transformedJsonEnvelopeStream.collect(toList());

        assertThat(transformedJsonEnvelopeList.size(), is(1));

        assertThat(transformedJsonEnvelopeList.get(0).metadata().name(), is(TRANSFORM_NAME));
    }

    @Test
    public void shouldTransformAndMove() {

        final JsonEnvelope event = buildEnvelope(EVENT_NAME);
        final JsonEnvelope event1 = buildEnvelope(EVENT_NAME_PASS);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event, event1);

        final Stream<JsonEnvelope> transformedJsonEnvelopeStream = streamTransformerUtil.transformAndMove(jsonEnvelopeStream, newHashSet(new TestTransformation()));

        final List<JsonEnvelope> transformedJsonEnvelopeList = transformedJsonEnvelopeStream.collect(toList());

        assertThat(transformedJsonEnvelopeList.size(), is(1));

        assertThat(transformedJsonEnvelopeList.get(0).metadata().name(), is(TRANSFORM_NAME));
    }

    @Test
    public void shouldNotTransform() {

        final JsonEnvelope event = buildEnvelope(EVENT_NAME_PASS);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event);

        final Stream<JsonEnvelope> transformedJsonEnvelopeStream = streamTransformerUtil.transform(jsonEnvelopeStream, newHashSet(new TestTransformation()));

        final List<JsonEnvelope> transformedJsonEnvelopeList = transformedJsonEnvelopeStream.collect(toList());

        assertThat(transformedJsonEnvelopeList.size(), is(1));

        assertThat(transformedJsonEnvelopeList.get(0).metadata().name(), is(EVENT_NAME_PASS));
    }

    @Test
    public void shouldFilterOriginalEvents() throws Exception {

        final JsonEnvelope jsonEnvelope = buildEnvelope("sample.events.name");
        final JsonEnvelope jsonEnvelope1 = buildEnvelope("should.not.apply");
        final JsonEnvelope jsonEnvelope2 = buildEnvelope("should.not.apply1");

        final List<JsonEnvelope> jsonEnvelopeList = streamTransformerUtil
                .filterOriginalEvents(asList(jsonEnvelope, jsonEnvelope1, jsonEnvelope2), newHashSet(new TestTransformation()))
                .collect(toList());

        assertThat(jsonEnvelopeList.size(), is(2));
        assertThat(jsonEnvelopeList.get(0).metadata().name(), is("should.not.apply"));
        assertThat(jsonEnvelopeList.get(1).metadata().name(), is("should.not.apply1"));
    }


    @Test
    public void shouldReturnEmptyStreamForOriginalEvents() throws Exception {
        final JsonEnvelope jsonEnvelope = buildEnvelope("sample.events.name");
        final JsonEnvelope jsonEnvelope1 = buildEnvelope("sample.events.name");

        final List<JsonEnvelope> jsonEnvelopeList = streamTransformerUtil
                .filterOriginalEvents(asList(jsonEnvelope, jsonEnvelope1), newHashSet(new TestTransformation()))
                .collect(toList());

        assertTrue(jsonEnvelopeList.isEmpty());
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
