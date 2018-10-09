package uk.gov.justice.tools.eventsourcing.transformation.service;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
import uk.gov.justice.tools.eventsourcing.transformation.SequenceValidationException;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;


public class EventTransformationRegistryTest {

    @Test
    public void shouldCreateSortedTransformationMap() throws Exception {

        final EventTransformationRegistry eventTransformationRegistry = new EventTransformationRegistry();

        final EventTransformationFoundEvent eventTransformationEvent1 = new EventTransformationFoundEvent(TestTransformation.class, 1);
        final EventTransformationFoundEvent eventTransformationEvent2 = new EventTransformationFoundEvent(TestTransformation.class, 2);
        final EventTransformationFoundEvent eventTransformationEvent3 = new EventTransformationFoundEvent(TestTransformation1.class, 1);

        eventTransformationRegistry.createTransformations(eventTransformationEvent2);
        eventTransformationRegistry.createTransformations(eventTransformationEvent1);
        eventTransformationRegistry.createTransformations(eventTransformationEvent3);

        assertThat(eventTransformationRegistry.getPasses().size(), is(2));

        final List<EventTransformation> eventTransformationList1 = new ArrayList<>(eventTransformationRegistry.getEventTransformationBy(1));
        final List<EventTransformation> eventTransformationList2 = new ArrayList<>(eventTransformationRegistry.getEventTransformationBy(2));

        final Optional<EventTransformation> transformation = eventTransformationList1
                .stream()
                .filter(eventTransformation -> eventTransformation instanceof TestTransformation)
                .findFirst();

        final Optional<EventTransformation> transformation1 = eventTransformationList1
                .stream()
                .filter(eventTransformation -> eventTransformation instanceof TestTransformation1)
                .findFirst();

        assertThat(eventTransformationList1.size(), is(2));
        assertTrue(transformation.isPresent());
        assertTrue(transformation1.isPresent());

        assertThat(eventTransformationList2.size(), is(1));
        assertThat(eventTransformationList2.get(0), instanceOf(TestTransformation.class));
    }

    @Test
    public void shouldThrowExceptionIfNotInSequence() throws InstantiationException, IllegalAccessException {
        final EventTransformationRegistry eventTransformationRegistry = new EventTransformationRegistry();

        final EventTransformationFoundEvent eventTransformationEvent1 = new EventTransformationFoundEvent(TestTransformation.class, 1);
        final EventTransformationFoundEvent eventTransformationEvent2 = new EventTransformationFoundEvent(TestTransformation.class, 3);

        eventTransformationRegistry.createTransformations(eventTransformationEvent1);
        eventTransformationRegistry.createTransformations(eventTransformationEvent2);

        try {
            eventTransformationRegistry.getPasses();
            fail();
        } catch (final SequenceValidationException expected) {
            assertThat(expected.getMessage(), is("Transformation passes are not in sequence [1, 3]"));
        }
    }

    @Test
    public void shouldReturnPassesInSequence() throws InstantiationException, IllegalAccessException {
        final EventTransformationRegistry eventTransformationRegistry = new EventTransformationRegistry();

        final EventTransformationFoundEvent eventTransformationEvent1 = new EventTransformationFoundEvent(TestTransformation.class, 1);
        final EventTransformationFoundEvent eventTransformationEvent2 = new EventTransformationFoundEvent(TestTransformation.class, 2);

        eventTransformationRegistry.createTransformations(eventTransformationEvent1);
        eventTransformationRegistry.createTransformations(eventTransformationEvent2);
        final List<Integer> passes = new ArrayList<>(eventTransformationRegistry.getPasses());

        assertThat(passes.get(0), is(1));
        assertThat(passes.get(1), is(2));
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
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {
            return Stream.of(event);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            // Do nothing
        }
    }

}

