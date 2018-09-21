package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.MOVE_AND_TRANSFORM;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamMoveFilterTest {

    private StreamMoveFilter streamMoveFilter;

    @Before
    public void setup() {

        final SampleTransformationMove sampleTransformationMove = new SampleTransformationMove();
        sampleTransformationMove.setEnveloper(createEnveloper());

        final Set<EventTransformation> eventTransformationSet = Sets.newSet(sampleTransformationMove);

        streamMoveFilter = new StreamMoveFilter(eventTransformationSet);
    }

    @Test
    public void shouldFilterMoveEvents() throws Exception {
        final JsonEnvelope jsonEnvelope = buildEnvelope("sample.events.name.move");
        final JsonEnvelope jsonEnvelope1 = buildEnvelope("should.not.apply.move");

        final List<JsonEnvelope> jsonEnvelopeList = streamMoveFilter.filterMoveEvents(asList(jsonEnvelope, jsonEnvelope1)).collect(toList());

        assertThat(jsonEnvelopeList.size(), is(1));
        assertThat(jsonEnvelopeList.get(0).metadata().name(), is("sample.events.transformedName.move"));
    }


    @Test
    public void shouldFilterNonMoveEvents() throws Exception {
        final JsonEnvelope jsonEnvelope = buildEnvelope("sample.events.name.move");
        final JsonEnvelope jsonEnvelope1 = buildEnvelope("should.not.apply.move");
        final JsonEnvelope jsonEnvelope2 = buildEnvelope("should.not.apply.move1");

        final List<JsonEnvelope> jsonEnvelopeList = streamMoveFilter.filterOriginalEvents(asList(jsonEnvelope, jsonEnvelope1, jsonEnvelope2)).collect(toList());

        assertThat(jsonEnvelopeList.size(), is(2));
        assertThat(jsonEnvelopeList.get(0).metadata().name(), is("should.not.apply.move"));
        assertThat(jsonEnvelopeList.get(1).metadata().name(), is("should.not.apply.move1"));
    }


    @Test
    public void shouldReturnEmptyStream() throws Exception {
        final JsonEnvelope jsonEnvelope = buildEnvelope("should.not.apply.move");
        final JsonEnvelope jsonEnvelope1 = buildEnvelope("should.not.apply.move1");
        final JsonEnvelope jsonEnvelope2 = buildEnvelope("should.not.apply.move2");

        final List<JsonEnvelope> jsonEnvelopeList = streamMoveFilter.filterMoveEvents(asList(jsonEnvelope, jsonEnvelope1, jsonEnvelope2)).collect(toList());

        assertTrue(jsonEnvelopeList.isEmpty());
    }

    @Test
    public void shouldReturnEmptyStreamForOriginalEvents() throws Exception {
        final JsonEnvelope jsonEnvelope = buildEnvelope("sample.events.name.move");
        final JsonEnvelope jsonEnvelope1 = buildEnvelope("sample.events.name.move");

        final List<JsonEnvelope> jsonEnvelopeList = streamMoveFilter.filterOriginalEvents(asList(jsonEnvelope, jsonEnvelope1)).collect(toList());

        assertTrue(jsonEnvelopeList.isEmpty());
    }


    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }

    @Transformation
    public class SampleTransformationMove implements EventTransformation {

        private Enveloper enveloper;

        @Override
        public Action actionFor(JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase("sample.events.name.move")) {

                return MOVE_AND_TRANSFORM;
            }
            return NO_ACTION;
        }

        @Override
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {
            final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, "sample.events.transformedName.move").apply(event.payload());
            return Stream.of(transformedEnvelope);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            this.enveloper = enveloper;
        }
    }
}




