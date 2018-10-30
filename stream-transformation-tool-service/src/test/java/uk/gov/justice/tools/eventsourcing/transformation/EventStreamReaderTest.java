package uk.gov.justice.tools.eventsourcing.transformation;


import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventStreamReaderTest {

    @Mock(answer = RETURNS_DEEP_STUBS)
    private EventSource eventSource;

    @InjectMocks
    private EventStreamReader eventStreamReader;

    @Test
    public void shouldRetrieveStreamById() throws Exception {
        final UUID streamId = randomUUID();
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(eventSource.getStreamById(streamId).read()).thenReturn(Stream.of(jsonEnvelope));

        final List<JsonEnvelope> jsonEnvelopeList = eventStreamReader.getStreamBy(streamId);

        assertThat(jsonEnvelopeList.get(0), is(jsonEnvelope));
    }

    @Test
    public void shouldReturnEmptyList() throws Exception {
        final UUID streamId = randomUUID();
        when(eventSource.getStreamById(streamId).read()).thenReturn(Stream.empty());

        final List<JsonEnvelope> jsonEnvelopeList = eventStreamReader.getStreamBy(streamId);

        assertTrue(jsonEnvelopeList.isEmpty());
    }

}