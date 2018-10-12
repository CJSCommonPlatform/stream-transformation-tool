package uk.gov.justice.tools.eventsourcing.transformation.service;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationStreamIdFilter;
import uk.gov.justice.tools.eventsourcing.transformation.StreamTransformerUtil;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@Ignore
@RunWith(MockitoJUnitRunner.class)
public class StreamTransformerTest {
//
//    private static final UUID STREAM_ID = randomUUID();
//    private static final UUID BACKUP_STREAM_ID = randomUUID();
//
//    private static final String SOURCE_EVENT_NAME = "test.event.name";
//    private static final String TRANSFORMED_EVENT_NAME = "test.event.newName";
//    private static final String OTHER_EVENT_NAME = "test.event.name2";
//
//    @Mock
//    private Logger logger;
//
//    @Mock
//    private EventSource eventSource;
//
//    @Mock
//    private EventStream eventStream;
//
//    @Mock
//    private EventTransformation eventTransformation;
//
//    @Mock
//    private StreamTransformerUtil streamTransformerUtil;
//
//    @Mock
//    private EventTransformationStreamIdFilter eventTransformationStreamIdFilter;
//
//    @Captor
//    private ArgumentCaptor<Stream<JsonEnvelope>> streamArgumentCaptor;
//
//    @Captor
//    private ArgumentCaptor<Set<EventTransformation>> eventTransformationArgumentCaptor;
//
//    @Captor
//    private ArgumentCaptor<JsonEnvelope> envelopeCaptor;
//
//    @Captor
//    private ArgumentCaptor<JsonEnvelope> envelopeCaptor2;
//
//    @Spy
//    private Enveloper enveloper = createEnveloper();
//
//    @InjectMocks
//    private StreamTransformer streamTransformer;
//
//
//    @Test
//    public void shouldTransformStreamOfSingleEventAndReturnBackupStreamId() throws EventStreamException {
//
//        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
//        final Set<EventTransformation> transformations = new HashSet<>();
//        transformations.add(eventTransformation);
//
//        given(eventSource.cloneStream(STREAM_ID)).willReturn(BACKUP_STREAM_ID);
//        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);
//        given(eventStream.read()).willReturn(Stream.of(event));
//        final List<JsonEnvelope> event1 = Stream.of(event).collect(Collectors.toList());
//        //given(streamTransformerUtil.transform(streamArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture())).willReturn(event1);
//        when(eventTransformationStreamIdFilter.getEventTransformationStreamId(transformations, event1)).thenReturn(Optional.of(UUID.randomUUID()));
//        //when(streamTransformerUtil.getTransformedStream()).thenReturn(event1);
//        given(eventTransformation.actionFor(any(JsonEnvelope.class))).willReturn(TRANSFORM);
//        given(eventTransformation.streamId(any(JsonEnvelope.class))).willReturn(Optional.of(UUID.randomUUID()));
//        given(eventTransformation.apply(event)).willReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));
//
//        final Optional<UUID> resultStreamId = streamTransformer.transformAndBackupStream(STREAM_ID, newHashSet(eventTransformation));
//
//        final InOrder inOrder = inOrder(eventSource, eventStream, eventTransformation);
//        inOrder.verify(eventSource).cloneStream(STREAM_ID);
//        inOrder.verify(eventSource).clearStream(STREAM_ID);
//        inOrder.verify(eventStream).append(streamArgumentCaptor.capture());
//
//        // todo can't get below assertions working as actionFor and apply methods are not
//        // being called at unit test level. Not sure if there's an issue with the way we have mocked objects
////        inOrder.verify(eventTransformation).actionFor(envelopeCaptor.capture());
////        inOrder.verify(eventTransformation).apply(envelopeCaptor2.capture());
////        final JsonEnvelope jsonEnvelope = envelopeCaptor.getValue();
////        assertThat(jsonEnvelope.metadata().streamId(), is(Optional.of(STREAM_ID)));
////        assertThat(jsonEnvelope.metadata().name(), is(SOURCE_EVENT_NAME));
////
////        final JsonEnvelope jsonEnvelope2 = envelopeCaptor2.getValue();
////        assertThat(jsonEnvelope2.metadata().streamId(), is(Optional.of(STREAM_ID)));
////        assertThat(jsonEnvelope2.metadata().name(), is(SOURCE_EVENT_NAME));
//
//        assertThat(resultStreamId, is(Optional.of(BACKUP_STREAM_ID)));
//    }
//
//    @Test
//    public void shouldNotTransformEventWhichHasNotBeenIndicatedFor() throws EventStreamException {
//        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
//        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
//        given(eventSource.cloneStream(STREAM_ID)).willReturn(BACKUP_STREAM_ID);
//        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);
//        given(eventStream.read()).willReturn(Stream.of(event, event2));
//        given(eventTransformation.actionFor(event)).willReturn(TRANSFORM);
//        given(eventTransformation.actionFor(event2)).willReturn(NO_ACTION);
//
//        streamTransformer.transformAndBackupStream(STREAM_ID, newHashSet(eventTransformation));
//
//        // todo can't get below assertions working as actionFor and apply methods are not
//        // being called at unit test level. Not sure if there's an issue with the way we have mocked objects
////        verify(eventTransformation).actionFor(envelopeCaptor.capture());
////        verify(eventTransformation).apply(envelopeCaptor2.capture());
////
////        final List<JsonEnvelope> events = envelopeCaptor.getAllValues();
////        assertThat(events, hasSize(1));
////        assertThat(events.get(0).metadata().streamId(), is(Optional.of(STREAM_ID)));
////        assertThat(events.get(0).metadata().name(), is(SOURCE_EVENT_NAME));
////
////        final List<JsonEnvelope> events2 = envelopeCaptor2.getAllValues();
////        assertThat(events2, hasSize(1));
////        assertThat(events2.get(0).metadata().streamId(), is(Optional.of(STREAM_ID)));
////        assertThat(events2.get(0).metadata().name(), is(SOURCE_EVENT_NAME));
//
//        verifyNoMoreInteractions(eventTransformation);
//    }
//
//    @Test
//    public void shouldLogErrorAndReturnEmptyStreamIdIfTransformAndBackUpStreamFailed() throws EventStreamException {
//
//        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
//        Set<EventTransformation> transformations = newHashSet(eventTransformation);
//
//        given(eventSource.cloneStream(STREAM_ID)).willReturn(BACKUP_STREAM_ID);
//        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);
//
//        when(eventStream.read()).thenReturn(Stream.of(event));
//        given(eventTransformation.actionFor(any(JsonEnvelope.class))).willReturn(TRANSFORM);
//        given(eventTransformation.apply(event)).willReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));
//
//        Optional<UUID> clonedStreamId =  streamTransformer.transformAndBackupStream(STREAM_ID, transformations);
//
//        assertThat(clonedStreamId, is(empty()));
//    }
//
//    @Test
//    public void shouldLogEventStreamExceptionAndReturnEmptyStreamIdIfTransformAndBackUpStreamFailed() throws Exception {
//
//        Set<EventTransformation> transformations = newHashSet(eventTransformation);
//        doThrow(EventStreamException.class).when(eventSource).cloneStream(any());
//
//        Optional<UUID> clonedStreamId = streamTransformer.transformAndBackupStream(STREAM_ID, transformations);
//
//        assertThat(clonedStreamId, is(empty()));
//    }
//
//    private JsonEnvelope buildEnvelope(final String eventName) {
//        return envelopeFrom(
//                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
//                createObjectBuilder().add("field", "value").build());
//    }
}
