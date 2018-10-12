package uk.gov.justice.tools.eventsourcing.transformation;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
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
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class TransformationCheckerTest {

    private static final String MOVE_EVENT_NAME = "sample.events.name.move";
    private static final String PASS_EVENT_NAME = "sample.events.name.pass";
    private static final String NEXT_EVENT_NAME = "sample.events.name.next";
    private static final String NO_BACKUP_EVENT_NAME = "sample.events.no.backup";

    final UUID STREAM_ID = randomUUID();

    @Mock
    private Logger logger;

    @Mock
    private EventTransformationRegistry eventTransformationRegistry;

    @InjectMocks
    private TransformationChecker transformationChecker;

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        final TestTransformation transformation1 = new TestTransformation();
        final TestTransformation1 transformation2 = new TestTransformation1();
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(transformation1, transformation2));
    }

    @Test
    public void shouldReturnNoActionWhenMoreThanOneTransformationAvailableForMove() throws Exception {
        final JsonEnvelope event = buildEnvelope(MOVE_EVENT_NAME);
        final JsonEnvelope event1 = buildEnvelope(PASS_EVENT_NAME);
        final Action action = transformationChecker.requiresTransformation(asList(event, event1), STREAM_ID, 1);

        assertTrue(action.equals(TRANSFORM));
    }

    @Test
    public void shouldReturnMoveAction() throws Exception {

        final TestTransformation transformation1 = new TestTransformation();

        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(transformation1));

        final JsonEnvelope event = buildEnvelope(MOVE_EVENT_NAME);
        final JsonEnvelope event1 = buildEnvelope(PASS_EVENT_NAME);
        final Action action = transformationChecker.requiresTransformation(asList(event, event1), STREAM_ID, 1);

        assertTrue(action.equals(TRANSFORM));
    }

    @Test
    public void shouldReturnTransformAction() throws Exception {

        final TestTransformation1 transformation1 = new TestTransformation1();

        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(transformation1));

        final JsonEnvelope event = buildEnvelope(PASS_EVENT_NAME);
        final JsonEnvelope event1 = buildEnvelope(MOVE_EVENT_NAME);
        final Action action = transformationChecker.requiresTransformation(asList(event, event1), STREAM_ID, 1);

        assertTrue(action.equals(TRANSFORM));
    }

    @Test
    public void shouldReturnTransformActionForDifferentTransformations() throws Exception {

        final TestTransformation1 transformation1 = new TestTransformation1();
        final TestTransformation2 transformation2 = new TestTransformation2();

        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(transformation1, transformation2));

        final JsonEnvelope event = buildEnvelope(PASS_EVENT_NAME);
        final JsonEnvelope event1 = buildEnvelope(NEXT_EVENT_NAME);
        final Action action = transformationChecker.requiresTransformation(asList(event, event1), STREAM_ID, 1);

        assertTrue(action.equals(TRANSFORM));
    }

    @Test
    public void shouldReturnNoActionWhenMoreThanOneTransformationAvailableForMoveAndNoBackUp() throws Exception {

        final TestTransformation1 transformation1 = new TestTransformation1();
        final TestTransformation3 transformation3 = new TestTransformation3();

        final JsonEnvelope event = buildEnvelope(NO_BACKUP_EVENT_NAME);
        final JsonEnvelope event1 = buildEnvelope(NEXT_EVENT_NAME);

        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(transformation1, transformation3));

        final Action action = transformationChecker.requiresTransformation(asList(event, event1), STREAM_ID, 1);

        assertTrue(action.equals(NO_ACTION));
    }


    @Transformation
    public static class TestTransformation implements EventTransformation {

        @Override
        public Action actionFor(JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase(MOVE_EVENT_NAME)) {
                return TRANSFORM;
            }
            return NO_ACTION;
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            //do nothing
        }
    }

    @Transformation
    public static class TestTransformation1 implements EventTransformation {

        @Override
        public Action actionFor(final JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase(PASS_EVENT_NAME)) {
                return TRANSFORM;
            }
            return NO_ACTION;
        }

        @Override
        public void setEnveloper(final Enveloper enveloper) {
            //do nothing
        }
    }

    @Transformation
    public static class TestTransformation2 implements EventTransformation {

        @Override
        public Action actionFor(final JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase(NEXT_EVENT_NAME)) {
                return TRANSFORM;
            }
            return NO_ACTION;
        }

        @Override
        public void setEnveloper(final Enveloper enveloper) {
            //do nothing
        }
    }

    @Transformation
    public static class TestTransformation3 implements EventTransformation {

        @Override
        public Action actionFor(final JsonEnvelope event) {
            if (event.metadata().name().equalsIgnoreCase(NEXT_EVENT_NAME)) {
                return new Action(false, false, false);
            }
            return NO_ACTION;
        }

        @Override
        public void setEnveloper(final Enveloper enveloper) {
            //do nothing
        }
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }


}