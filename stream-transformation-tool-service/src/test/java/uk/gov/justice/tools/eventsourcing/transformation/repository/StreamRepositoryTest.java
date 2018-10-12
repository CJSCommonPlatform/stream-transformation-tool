package uk.gov.justice.tools.eventsourcing.transformation.repository;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StreamRepositoryTest {

    private static final UUID STREAM_ID = randomUUID();

    @Mock
    private Logger logger;

    @Mock
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @InjectMocks
    private StreamRepository streamRepository;

    @Test
    public void shouldDeactivateTheStreamWhenRequested() {
        streamRepository.deactivateStream(STREAM_ID);

        verify(eventStreamJdbcRepository).markActive(STREAM_ID, false);
        verifyNoMoreInteractions(eventStreamJdbcRepository);
    }

    @Test
    public void shouldDeleteTheStreamWhenRequested() {
        streamRepository.deleteStream(STREAM_ID);

        verify(eventStreamJdbcRepository).delete(STREAM_ID);
        verifyNoMoreInteractions(eventStreamJdbcRepository);
    }

    @Test
    public void shouldCreateStreamIfNeeded() {
        final UUID streamId = streamRepository.createStream();

        verify(eventStreamJdbcRepository).insert(streamId);
        verifyNoMoreInteractions(eventStreamJdbcRepository);
    }

}
