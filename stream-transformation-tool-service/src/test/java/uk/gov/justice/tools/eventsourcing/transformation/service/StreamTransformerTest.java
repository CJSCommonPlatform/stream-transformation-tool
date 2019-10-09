package uk.gov.justice.tools.eventsourcing.transformation.service;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventSourceTransformation;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.StreamTransformerUtil;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StreamTransformerTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final UUID BACKUP_STREAM_ID = randomUUID();

    private static final String SOURCE_EVENT_NAME = "test.event.name";
    private static final String TRANSFORMED_EVENT_NAME = "test.event.newName";
    private static final String OTHER_EVENT_NAME = "test.event.name2";

    @Mock
    private Logger logger;

    @Mock
    private EventSource eventSource;

    @Mock
    private EventSourceTransformation eventSourceTransformation;

    @Mock
    private EventStream eventStream;

    @Mock
    private EventTransformation eventTransformation;

    @Mock
    private StreamTransformerUtil streamTransformerUtil;

    @Mock
    private StreamAppender streamAppender;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> streamArgumentCaptor;

    @Captor
    private ArgumentCaptor<Set<EventTransformation>> eventTransformationArgumentCaptor;

    @Captor
    private ArgumentCaptor<UUID> streamIdArgumentCaptor;

    @InjectMocks
    private StreamTransformer streamTransformer;

    @Test
    public void shouldTransformStreamOfSingleEventAndReturnBackupStreamId() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);

        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);
        given(eventStream.read()).willReturn(Stream.of(event));
        given(streamTransformerUtil.transform(streamArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture())).willReturn(Stream.of(event));
        given(eventTransformation.actionFor(any(JsonEnvelope.class))).willReturn(TRANSFORM);
        given(eventTransformation.apply(event)).willReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));

        streamTransformer.transformStream(STREAM_ID, newHashSet(eventTransformation));

        final InOrder inOrder = inOrder(eventSourceTransformation, eventStream, eventTransformation, streamAppender);

        inOrder.verify(eventSourceTransformation).clearStream(STREAM_ID);
        inOrder.verify(streamAppender).appendEventsToStream(streamIdArgumentCaptor.capture(), streamArgumentCaptor.capture());
    }

    @Test
    public void shouldNotTransformEventWhichHasNotBeenIndicatedFor() throws EventStreamException {
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);
        given(eventStream.read()).willReturn(Stream.of(event, event2));
        given(eventTransformation.actionFor(event)).willReturn(TRANSFORM);
        given(eventTransformation.actionFor(event2)).willReturn(NO_ACTION);

        streamTransformer.transformStream(STREAM_ID, newHashSet(eventTransformation));

        verifyNoMoreInteractions(eventTransformation);
    }

    @Test(expected = EventStreamException.class)
    public void shouldNotSuppressEventStreamException() throws Exception {
        final Set<EventTransformation> transformations = newHashSet(eventTransformation);
        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final JsonEnvelope event2 = buildEnvelope(OTHER_EVENT_NAME);
        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);
        given(eventStream.read()).willReturn(Stream.of(event, event2));
        doThrow(EventStreamException.class).when(eventSourceTransformation).clearStream(any());
        streamTransformer.transformStream(STREAM_ID, transformations);
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
}
