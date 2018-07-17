package uk.gov.justice.tools.eventsourcing.transformation.service;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.ARCHIVE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.DEACTIVATE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.slf4j.Logger;

@RunWith(DataProviderRunner.class)
public class EventStreamTransformationServiceTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final UUID BACKUP_STREAM_ID = randomUUID();

    private static final String SOURCE_EVENT_NAME = "test.event.name";
    private static final String TRANSFORMED_EVENT_NAME = "test.event.newName";
    private static final String OTHER_EVENT_NAME = "test.event.name2";

    @DataProvider
    public static Object[][] provideArchiveAndDeactivateActions() {
        return new Object[][]{
                {ARCHIVE},
                {DEACTIVATE}
        };
    }

    @DataProvider
    public static Object[][] provideNoActionCombinations() {
        return new Object[][]{
                {NO_ACTION},
                {null}
        };
    }

    @Mock
    private Logger logger;

    @Mock
    private EventSource eventSource;

    @Mock
    private EventStream eventStream;

    @Mock
    private EventTransformation eventTransformation;

    @Mock
    private StreamTransformer streamTransformer;

    @Mock
    private StreamRepository streamRepository;

    @Mock
    private EventJdbcRepository eventRepository;

    @InjectMocks
    private EventStreamTransformationService underTest;

    @Captor
    private ArgumentCaptor<JsonEnvelope> envelopeCaptor;

    @Before
    public void setup() {
        initMocks(this);

        underTest.transformations = newHashSet(eventTransformation);

        when(eventTransformation.apply(any(JsonEnvelope.class))).thenReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));
        when(logger.isDebugEnabled()).thenReturn(true);

        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream);
    }

    @Test
    public void shouldTransformStreamOfSingleEvent() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(TRANSFORM);
        when(eventStream.read()).thenReturn(Stream.of(event));

        underTest.transformEventStream(STREAM_ID);

        final InOrder inOrder = inOrder(eventSource, eventStream, eventTransformation, streamTransformer);
        inOrder.verify(eventSource).getStreamById(STREAM_ID);
        inOrder.verify(eventStream).read();
        inOrder.verify(eventTransformation).actionFor(envelopeCaptor.capture());
        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, underTest.transformations);

        final JsonEnvelope jsonEnvelope = envelopeCaptor.getValue();
        assertThat(jsonEnvelope.metadata().name(), is(SOURCE_EVENT_NAME));
        assertThat(jsonEnvelope.metadata().streamId().isPresent(), is(true));
        jsonEnvelope.metadata().streamId().ifPresent(streamId -> assertThat(streamId, is(STREAM_ID)));

        verifyZeroInteractions(streamRepository, eventRepository);
    }

    @Test
    @UseDataProvider("provideArchiveAndDeactivateActions")
    public void shouldDeactivateStreamOfSingleEvent(final Action action) {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(action);
        when(eventStream.read()).thenReturn(Stream.of(event));

        underTest.transformEventStream(STREAM_ID);

        verify(streamRepository).deactivateStream(STREAM_ID);
        verifyNoMoreInteractions(streamRepository);
        verifyZeroInteractions(streamTransformer, eventRepository);
    }

    @Test
    public void shouldTransformAllEventsOnStream() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        when(eventStream.read()).thenReturn(Stream.of(event, event2));
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(TRANSFORM);

        underTest.transformEventStream(STREAM_ID);

        final InOrder inOrder = inOrder(eventSource, eventStream, eventTransformation, streamTransformer);
        inOrder.verify(eventSource).getStreamById(STREAM_ID);
        inOrder.verify(eventStream).read();
        inOrder.verify(eventTransformation, times(2)).actionFor(envelopeCaptor.capture());
        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, underTest.transformations);

        final List<JsonEnvelope> jsonEnvelope = envelopeCaptor.getAllValues();
        assertThat(jsonEnvelope.get(0).metadata().name(), is(SOURCE_EVENT_NAME));
        assertThat(jsonEnvelope.get(0).metadata().streamId().isPresent(), is(true));
        jsonEnvelope.get(0).metadata().streamId().ifPresent(streamId -> assertThat(streamId, is(STREAM_ID)));

        assertThat(jsonEnvelope.get(1).metadata().name(), is(OTHER_EVENT_NAME));
        assertThat(jsonEnvelope.get(1).metadata().streamId().isPresent(), is(true));
        jsonEnvelope.get(1).metadata().streamId().ifPresent(streamId -> assertThat(streamId, is(STREAM_ID)));

        verifyZeroInteractions(streamRepository, eventRepository);
    }

    @Test
    public void shouldDeactivateStreamOnlyOnceIrrespectiveOfNoOfEventsOnStream() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        when(eventStream.read()).thenReturn(Stream.of(event, event2));
        when(eventTransformation.actionFor(any())).thenReturn(DEACTIVATE);

        underTest.transformEventStream(STREAM_ID);

        verify(streamRepository).deactivateStream(STREAM_ID);
        verifyNoMoreInteractions(streamRepository);
        verifyZeroInteractions(streamTransformer, eventRepository);
    }

    @Test
    @UseDataProvider("provideNoActionCombinations")
    public void shouldNotPerformAnyActionOnTheStreamIfNotIndicated(final Action action) {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformation.actionFor(any())).thenReturn(action);

        underTest.transformEventStream(STREAM_ID);

        verifyZeroInteractions(streamTransformer, streamRepository, eventRepository);
    }

    @Test
    public void shouldNotPerformAnyActionIfMultipleActionsAreDefinedOnAStream() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        when(eventStream.read()).thenReturn(Stream.of(event, event2));
        when(eventTransformation.actionFor(event)).thenReturn(DEACTIVATE);
        when(eventTransformation.actionFor(event2)).thenReturn(TRANSFORM);

        underTest.transformEventStream(STREAM_ID);

        verifyZeroInteractions(streamTransformer, streamRepository, eventRepository);
    }

    @Test
    public void shouldRegisterTransformation() throws InstantiationException, IllegalAccessException {
        underTest.transformations = new HashSet<>();
        final EventTransformationFoundEvent eventTransformationEvent = new EventTransformationFoundEvent(TestTransformation.class);

        underTest.register(eventTransformationEvent);

        assertThat(underTest.transformations, hasSize(1));
        underTest.transformations.stream().findFirst().ifPresent(transformation ->
                assertThat(transformation, instanceOf(TestTransformation.class)));
    }

    @Test
    public void shouldPerformAllTheIndicatedActionsOnAStream() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(
                new Action(true, true, false)
        );
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(streamTransformer.transformAndBackupStream(any(UUID.class), any())).thenReturn(of(BACKUP_STREAM_ID));

        underTest.transformEventStream(STREAM_ID);

        final InOrder inOrder = inOrder(streamTransformer, streamRepository, eventRepository);
        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, underTest.transformations);
        inOrder.verify(streamRepository).deleteStream(BACKUP_STREAM_ID);
        inOrder.verify(eventRepository).clear(BACKUP_STREAM_ID);
        inOrder.verify(streamRepository).deactivateStream(STREAM_ID);
        verifyNoMoreInteractions(streamTransformer, streamRepository, eventRepository);
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return DefaultJsonEnvelopeProvider.provider().envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
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
    }
}
