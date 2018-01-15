package uk.gov.justice.event.tool;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.event.tool.task.StreamTransformationTask;
import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationService;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import javax.enterprise.concurrent.ManagedExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StartTransformationTest {

    @Mock
    private ManagedExecutorService executorService;

    @Mock
    private JdbcEventRepository jdbcEventRepository;

    @Mock
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @Mock
    private EventStreamTransformationService eventStreamTransformationService;

    @InjectMocks
    private StartTransformation startTransformation;

    @Mock
    private EventStream eventStream;

    @Mock
    private EventStream eventStream2;

    @Before
    public void setup() {
        when(eventStreamJdbcRepository.findActive()).thenReturn(Stream.of(eventStream, eventStream2));
    }

    @Test
    public void shouldCreateTasksForEachStream() {
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(mock(Future.class));

        startTransformation.go();

        assertThat(startTransformation.outstandingTasks.size(), is(2));
        assertTrue(startTransformation.allTasksCreated);
    }

    @Test
    public void shouldRemoveFinishedTasks() {
        Future future = mock(Future.class);
        Future future2 = mock(Future.class);
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(future).thenReturn(future2);

        startTransformation.go();

        assertThat(startTransformation.outstandingTasks.size(), is(2));

        startTransformation.taskAborted(future, null, null, null);
        assertThat(startTransformation.outstandingTasks.size(), is(1));

        startTransformation.taskDone(future2, null, null, null);
        assertThat(startTransformation.outstandingTasks.size(), is(0));
    }

}