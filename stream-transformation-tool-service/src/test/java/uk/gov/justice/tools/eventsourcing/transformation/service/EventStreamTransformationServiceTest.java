package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.ARCHIVE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.TRANSFORM_EVENT;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;
import uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class EventStreamTransformationServiceTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final UUID CLONED_STREAM_ID = randomUUID();

    @Mock
    private EventSource eventSource;

    @Mock
    private EventStream eventStream;

    @Mock
    private EventStream clonedEventStream;

    @Mock
    private EventTransformation eventTransformation;

    @Mock
    private Logger logger;

    @Mock
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @InjectMocks
    private EventStreamTransformationService service;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> streamCaptor;

    private Enveloper enveloper = EnveloperFactory.createEnveloper();

    @Before
    public void setup() throws EventStreamException {
        final HashSet<EventTransformation> transformations = new HashSet<>();
        transformations.add(eventTransformation);
        service.transformations = transformations;
        service.enveloper = enveloper;

        mockTransformationMatcher();

        when(eventSource.cloneStream(STREAM_ID)).thenReturn(CLONED_STREAM_ID);
        when(eventSource.getStreamById(CLONED_STREAM_ID)).thenReturn(clonedEventStream);
        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream).thenReturn(eventStream);
    }

    @Test
    public void shouldTransformStreamOfSingleEvent() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope("test.event.name");
        when(eventTransformation.action(any())).thenReturn(TRANSFORM_EVENT);
        when(eventStream.read()).thenReturn(Stream.of(event)).thenReturn(Stream.of(event));

        service.transformEventStream(STREAM_ID);

        verify(eventSource).cloneStream(STREAM_ID);
        verify(eventSource).clearStream(STREAM_ID);
        verify(eventStream).append(streamCaptor.capture());
        verify(eventStream).append(any());
    }

    @Test
    public void shouldArchiveStreamOfSingleEvent() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope("test.event.name");
        when(eventTransformation.action(any())).thenReturn(ARCHIVE);
        when(eventStream.read()).thenReturn(Stream.of(event)).thenReturn(Stream.of(event));

        service.transformEventStream(STREAM_ID);
        verify(eventStreamJdbcRepository).markActive(STREAM_ID,false);
    }

    @Test
    public void shouldOnlyTransformOneEventOnStream() throws EventStreamException {
        when(eventTransformation.isApplicable(any())).thenReturn(true).thenReturn(true).thenReturn(false);

        final JsonEnvelope event = buildEnvelope("test.event.name");
        final JsonEnvelope event2 = buildEnvelope("test.event.name2");
        when(eventStream.read()).thenReturn(Stream.of(event, event2)).thenReturn(Stream.of(event, event2));
        when(eventTransformation.action(any())).thenReturn(TRANSFORM_EVENT);

        service.transformEventStream(STREAM_ID);

        verify(eventSource).clearStream(STREAM_ID);
        verify(eventStream).append(any());
    }

    @Test
    public void shouldOnlyArchiveOneEventOnStream() throws EventStreamException {
        when(eventTransformation.isApplicable(any())).thenReturn(true).thenReturn(true).thenReturn(false);

        final JsonEnvelope event = buildEnvelope("test.event.name");
        final JsonEnvelope event2 = buildEnvelope("test.event.name2");
        when(eventStream.read()).thenReturn(Stream.of(event, event2)).thenReturn(Stream.of(event, event2));
        when(eventTransformation.action(any())).thenReturn(ARCHIVE);

        service.transformEventStream(STREAM_ID);

        verify(eventStreamJdbcRepository).markActive(STREAM_ID,false);
    }


    @Test
    public void shouldNotPerformTransformationIfNotRequired() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope("test.event.name");
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformation.isApplicable(any())).thenReturn(false);

        service.transformEventStream(STREAM_ID);

        verify(eventSource).getStreamById(STREAM_ID);
        verify(eventStream).read();
        verifyZeroInteractions(clonedEventStream);
    }

    @Test
    public void shouldNotPerformArchiveIfNotRequired() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope("test.event.name");
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformation.action(any())).thenReturn(NO_ACTION);

        service.transformEventStream(STREAM_ID);

        verify(eventSource).getStreamById(STREAM_ID);
        verify(eventStream).read();
        verifyZeroInteractions(clonedEventStream);
    }

    @Test
    public void shouldNotPerformArchiveIfNullAction() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope("test.event.name");
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformation.action(any())).thenReturn(null);

        service.transformEventStream(STREAM_ID);

        verify(eventSource).getStreamById(STREAM_ID);
        verify(eventStream).read();
        verifyZeroInteractions(clonedEventStream);
    }

    @Test
    public void shouldNotPerformAnyActionIfMultipleActionDefined() throws EventStreamException {
        when(eventTransformation.isApplicable(any())).thenReturn(true).thenReturn(true).thenReturn(false);

        final JsonEnvelope event = buildEnvelope("test.event.name");
        final JsonEnvelope event2 = buildEnvelope("test.event.name2");
        when(eventStream.read()).thenReturn(Stream.of(event, event2)).thenReturn(Stream.of(event, event2));
        when(eventTransformation.action(event)).thenReturn(ARCHIVE);
        when(eventTransformation.action(event2)).thenReturn(TRANSFORM_EVENT);

        service.transformEventStream(STREAM_ID);

        verify(eventSource).getStreamById(STREAM_ID);
        verify(eventStream).read();
        verifyZeroInteractions(clonedEventStream);
    }

    @Test
    public void shouldRegisterTransformation() throws InstantiationException, IllegalAccessException {
        service.transformations = new HashSet<>();

        final EventTransformationFoundEvent eventTransformationEvent = new EventTransformationFoundEvent(TestTransformation.class);
        service.register(eventTransformationEvent);

        assertThat(service.transformations.size(), is(1));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return DefaultJsonEnvelopeProvider.provider().envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }

    private void mockTransformationMatcher() {
        when(eventTransformation.isApplicable(any())).thenReturn(true);
        when(eventTransformation.apply(any())).thenReturn(Stream.of(buildEnvelope("test.event.newName")));
    }

    @Transformation
    public static class TestTransformation implements EventTransformation {

        @Override
        public boolean isApplicable(JsonEnvelope event) {
            return false;
        }

        @Override
        public Stream<JsonEnvelope> apply(JsonEnvelope event) {
            return Stream.of(event);
        }

        @Override
        public void setEnveloper(Enveloper enveloper) {
            // Do nothing
        }
    }

//    @Test
//    public void doesNotContain() {
//        verify(eventStream).append(argThat(streamThat(not(hasItem(Changes.FOUR)))));
//    }

    private static <T> Matcher<Stream<T>> streamThat(Matcher<Iterable<? super T>> toMatch) {
        return new IterableStream<>(toMatch);
    }

    private static class IterableStream<T> extends TypeSafeMatcher<Stream<T>> {

        Matcher<Iterable<? super T>> toMatch;
        List<T> input = null;

        public IterableStream(Matcher<Iterable<? super T>> toMatch) {
            this.toMatch = toMatch;
        }

        @Override
        protected synchronized boolean matchesSafely(Stream<T> item) {
            // This is to protect against JUnit calling this more than once
            input = input == null ? item.collect(Collectors.toList()) : input;
            return toMatch.matches(input);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("stream that represents ");
            toMatch.describeTo(description);
        }
    }
}