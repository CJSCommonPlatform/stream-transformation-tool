package uk.gov.justice.event.tool;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.event.tool.task.StreamTransformationTask;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
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
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StartTransformationTest {

    @Mock
    private ManagedExecutorService executorService;

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

    @Mock
    private PassesDeterminer passesDeterminer;

    @Mock
    private EventTransformationRegistry eventTransformationRegistry;

    @Mock
    private Logger logger;

    @Before
    public void setup() {
        when(eventStreamJdbcRepository.findActive()).thenReturn(Stream.of(eventStream, eventStream2));
        when(passesDeterminer.getPassValue()).thenReturn(1).thenReturn(2);
        when(passesDeterminer.isLastElementInPasses()).thenReturn(true);
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
        when(eventStreamJdbcRepository.findActive()).thenReturn(Stream.of(eventStream, eventStream2));
        when(passesDeterminer.getPassValue()).thenReturn(1);

        final Future future = mock(Future.class);
        final Future future2 = mock(Future.class);
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(future).thenReturn(future2);

        startTransformation.go();

        assertThat(startTransformation.outstandingTasks.size(), is(2));

        startTransformation.taskAborted(future, null, null, mock(Throwable.class));
        assertThat(startTransformation.outstandingTasks.size(), is(1));

        startTransformation.taskDone(future2, null, null, null);
        assertThat(startTransformation.outstandingTasks.size(), is(0));
    }

    @Test
    public void shouldRemoveFinishedTaskForAllPasses(){
        when(eventStreamJdbcRepository.findActive()).thenReturn(Stream.of(eventStream, eventStream2)).thenReturn(Stream.of(eventStream2));
        when(passesDeterminer.getPassValue()).thenReturn(1).thenReturn(2);
        when(passesDeterminer.isLastElementInPasses()).thenReturn(false).thenReturn(true);


        final Future future = mock(Future.class);
        final Future future2 = mock(Future.class);
        final Future future3 = mock(Future.class);
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(future).thenReturn(future2).thenReturn(future3);

        startTransformation.go();

        startTransformation.taskStarting(future, null, null);
        verify(logger).debug("Starting Transformation task");

        startTransformation.taskSubmitted(future, null, null);
        verify(logger).debug("Submitted Transformation task");

        assertThat(startTransformation.outstandingTasks.size(), is(2));

        startTransformation.taskAborted(future, null, null, mock(Throwable.class));
        assertThat(startTransformation.outstandingTasks.size(), is(1));

        startTransformation.taskDone(future2, null, null, null);

        startTransformation.taskDone(future3, null, null, null);
        assertThat(startTransformation.outstandingTasks.size(), is(0));
    }

}