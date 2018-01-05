package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;
import uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

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
        when(eventStream.read()).thenReturn(Stream.of(event));

        final Stream<JsonEnvelope> stream = Stream.of(event);
        service.transformEventStream(stream);

        verify(eventSource).cloneStream(STREAM_ID);
        verify(eventSource).clearStream(STREAM_ID);
        verify(eventStream).append(streamCaptor.capture());
        Stream<JsonEnvelope> value = streamCaptor.getValue();
        Optional<JsonEnvelope> first = value.findFirst();
        assertTrue(first.isPresent());
        assertThat(first.get().metadata().name(), is("test.event.newName"));
    }

    @Test
    public void shouldOnlyTransformOneEventOnStream() throws EventStreamException {
        when(eventTransformation.isApplicable(any())).thenReturn(true).thenReturn(true).thenReturn(false);

        final JsonEnvelope event = buildEnvelope("test.event.name");
        final JsonEnvelope event2 = buildEnvelope("test.event.name2");
        when(eventStream.read()).thenReturn(Stream.of(event, event2));

        Stream<JsonEnvelope> stream = Stream.of(event, event2);
        service.transformEventStream(stream);

        verify(eventSource).clearStream(STREAM_ID);
        verify(eventStream).append(streamCaptor.capture());
        Stream<JsonEnvelope> value = streamCaptor.getValue();

        List<JsonEnvelope> collect = value.collect(toList());
        assertThat(collect.size(), is(2));

        assertThat(collect.get(0).metadata().name(), is("test.event.newName"));
        assertThat(collect.get(1).metadata().name(), is("test.event.name2"));
    }

    @Test
    public void shouldNotPerformTransformationIfNotRequired() throws EventStreamException {
        when(eventTransformation.isApplicable(any())).thenReturn(false);

        final JsonEnvelope event = buildEnvelope("test.event.name");

        Stream<JsonEnvelope> stream = Stream.of(event);
        service.transformEventStream(stream);

        verifyZeroInteractions(clonedEventStream);
        verifyZeroInteractions(eventStream);
        verifyZeroInteractions(eventSource);
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
}