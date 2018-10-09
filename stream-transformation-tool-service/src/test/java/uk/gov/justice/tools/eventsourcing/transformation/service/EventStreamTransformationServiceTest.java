package uk.gov.justice.tools.eventsourcing.transformation.service;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.ARCHIVE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.DEACTIVATE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.MOVE_AND_TRANSFORM;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
import uk.gov.justice.tools.eventsourcing.transformation.StreamMover;
import uk.gov.justice.tools.eventsourcing.transformation.TransformationChecker;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

import java.util.Optional;
import java.util.Set;
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
    private StreamMover streamMover;

    @Mock
    private StreamRepository streamRepository;

    @Mock
    private EventJdbcRepository eventRepository;

    @Mock
    private EventTransformationRegistry eventTransformationRegistry;

    @Mock
    private TransformationChecker transformationChecker;

    @InjectMocks
    private EventStreamTransformationService eventStreamTransformationService;

    @Captor
    private ArgumentCaptor<JsonEnvelope> envelopeCaptor;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> streamArgumentCaptor;

    @Captor
    private ArgumentCaptor<UUID> uuidCaptor;

    @Captor
    private ArgumentCaptor<Integer>intArgumentCaptor;

    @Before
    public void setup() {
        initMocks(this);

        when(eventTransformation.apply(any(JsonEnvelope.class))).thenReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));
        when(logger.isDebugEnabled()).thenReturn(true);

        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream);
    }

    @Test
    public void shouldTransformStreamOfSingleEvent() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        Set<EventTransformation> transformations = newHashSet(eventTransformation);

        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(transformations);
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(TRANSFORM);

        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        final InOrder inOrder = inOrder(eventSource, eventStream, eventTransformationRegistry, transformationChecker, streamTransformer);
        inOrder.verify(eventSource).getStreamById(STREAM_ID);
        inOrder.verify(eventStream).read();
        inOrder.verify(eventTransformationRegistry).getEventTransformationBy(1);

        inOrder.verify(transformationChecker).requiresTransformation(streamArgumentCaptor.capture(),
                uuidCaptor.capture(), intArgumentCaptor.capture());

        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, newHashSet(eventTransformation));

        verifyZeroInteractions(streamRepository, eventRepository);
    }

    @Test
    public void shouldMoveTransformStreamOfSingleEvent() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        Set<EventTransformation> transformations = newHashSet(eventTransformation);

        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(transformations);
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(MOVE_AND_TRANSFORM);

        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        final InOrder inOrder = inOrder(eventSource, eventStream, eventTransformationRegistry, transformationChecker, streamMover);
        inOrder.verify(eventSource).getStreamById(STREAM_ID);
        inOrder.verify(eventStream).read();
        inOrder.verify(eventTransformationRegistry).getEventTransformationBy(1);

        inOrder.verify(transformationChecker).requiresTransformation(streamArgumentCaptor.capture(),
                uuidCaptor.capture(), intArgumentCaptor.capture());

        inOrder.verify(streamMover).moveAndBackupStream(STREAM_ID, newHashSet(eventTransformation));

        verifyZeroInteractions(streamRepository, eventRepository);
    }

    @Test
    @UseDataProvider("provideArchiveAndDeactivateActions")
    public void shouldDeactivateStreamOfSingleEvent(final Action action) {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(action);
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(DEACTIVATE);
        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        verify(streamRepository).deactivateStream(STREAM_ID);
        verifyNoMoreInteractions(streamRepository);
        verifyZeroInteractions(streamTransformer, eventRepository);
    }

    @Test
    public void shouldTransformAllEventsOnStream() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        when(eventStream.read()).thenReturn(Stream.of(event, event2));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(TRANSFORM);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(TRANSFORM);

        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        final InOrder inOrder = inOrder(eventSource, eventStream, eventTransformationRegistry, transformationChecker, streamTransformer);
        inOrder.verify(eventSource).getStreamById(STREAM_ID);
        inOrder.verify(eventStream).read();
        inOrder.verify(eventTransformationRegistry).getEventTransformationBy(1);
        inOrder.verify(transformationChecker).requiresTransformation(streamArgumentCaptor.capture(),
                uuidCaptor.capture(), intArgumentCaptor.capture());
        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, newHashSet(eventTransformation));

        verifyZeroInteractions(streamRepository, eventRepository);
    }

    @Test
    public void shouldDeactivateStreamOnlyOnceIrrespectiveOfNoOfEventsOnStream() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        final int pass = 1;

        when(eventStream.read()).thenReturn(Stream.of(event, event2));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(DEACTIVATE);

        eventStreamTransformationService.transformEventStream(STREAM_ID, pass);

        verify(streamRepository).deactivateStream(STREAM_ID);
        verifyNoMoreInteractions(streamRepository);
        verifyZeroInteractions(streamTransformer, eventRepository);
    }

    @Test
    @UseDataProvider("provideNoActionCombinations")
    public void shouldNotPerformAnyActionOnTheStreamIfNotIndicated(final Action action) {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final int pass = 1;
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(NO_ACTION);
        when(eventTransformation.actionFor(any())).thenReturn(action);

        eventStreamTransformationService.transformEventStream(STREAM_ID, pass);

        verifyZeroInteractions(streamTransformer, streamRepository, eventRepository);
    }

    @Test
    public void shouldNotPerformAnyActionIfMultipleActionsAreDefinedOnAStream() {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);


        when(eventStream.read()).thenReturn(Stream.of(event, event2));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));

        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(NO_ACTION);

        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        verifyZeroInteractions(streamTransformer, streamRepository, eventRepository);
    }


    @Test
    public void shouldPerformAllTheIndicatedActionsOnAStream() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Action action = new Action(true, true, false, false);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(
                action
        );
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(action);
        when(streamTransformer.transformAndBackupStream(any(UUID.class), any())).thenReturn(of(BACKUP_STREAM_ID));

        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        final InOrder inOrder = inOrder(transformationChecker, streamTransformer, streamRepository);

        inOrder.verify(transformationChecker).requiresTransformation(streamArgumentCaptor.capture(),
                uuidCaptor.capture(), intArgumentCaptor.capture());

        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, newHashSet(eventTransformation));

        inOrder.verify(streamRepository).deleteStream(BACKUP_STREAM_ID);
        inOrder.verify(streamRepository).deactivateStream(STREAM_ID);
        verifyNoMoreInteractions(streamTransformer, streamRepository, eventRepository);
    }

    @Test
    public void shouldLogWhenDeleteCannotBePerformedWhenBackupStreamIdDoesNotExist() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Optional<UUID> noUUID = Optional.empty();
        final Action action = new Action(true, true, false, false);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(
                action
        );
        when(eventStream.read()).thenReturn(Stream.of(event));
        when(eventTransformationRegistry.getEventTransformationBy(1)).thenReturn(newHashSet(eventTransformation));
        given(transformationChecker.requiresTransformation(streamArgumentCaptor.capture(), uuidCaptor.capture(),
                intArgumentCaptor.capture())).willReturn(action);
        when(streamTransformer.transformAndBackupStream(any(UUID.class), any())).thenReturn(noUUID);

        eventStreamTransformationService.transformEventStream(STREAM_ID, 1);

        final InOrder inOrder = inOrder(transformationChecker, streamTransformer);

        inOrder.verify(transformationChecker).requiresTransformation(streamArgumentCaptor.capture(),
                uuidCaptor.capture(), intArgumentCaptor.capture());

        inOrder.verify(streamTransformer).transformAndBackupStream(STREAM_ID, newHashSet(eventTransformation));

        verify(logger).info(String.format("Cannot delete backup stream. No backup stream was created for stream '%s'", STREAM_ID));

        verifyNoMoreInteractions(streamTransformer);
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
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
