package uk.gov.justice.event.tool;

import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.event.tool.task.StreamTransformationTask;
import uk.gov.justice.services.eventsourcing.repository.jdbc.EventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;
import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationService;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.enterprise.concurrent.ManagedExecutorService;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StartTransformationTest {

    private Field streamsProcessedCountStepInfo;
    private Field streamsDoneCount;

    @Mock
    private ManagedExecutorService executorService;

    @Mock
    private EventRepository eventRepository;

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

    @Mock
    private StopWatch stopWatch;

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        when(passesDeterminer.getPassValue()).thenReturn(1).thenReturn(2);
        when(passesDeterminer.isLastElementInPasses()).thenReturn(true);
        streamsProcessedCountStepInfo = startTransformation.getClass().getDeclaredField("streamCountReportingInterval");
        streamsProcessedCountStepInfo.setAccessible(true);
        streamsProcessedCountStepInfo.set(startTransformation, 10);

        streamsDoneCount = startTransformation.getClass().getDeclaredField("processedStreamsCount");
        streamsDoneCount.setAccessible(true);
        streamsDoneCount.set(startTransformation, new AtomicInteger(0));
    }

    @Test
    public void shouldCreateTasksForEachStream() {

        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();

        when(eventRepository.getAllActiveStreamIds()).thenReturn(Stream.of(streamId_1, streamId_2));
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(mock(Future.class));

        startTransformation.go();

        assertThat(startTransformation.outstandingTasks.size(), is(2));
        assertTrue(startTransformation.allTasksCreated);
    }

    @Test
    public void shouldRemoveFinishedTasks() {

        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();

        when(eventRepository.getAllActiveStreamIds()).thenReturn(Stream.of(streamId_1, streamId_2));
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
    public void shouldRemoveFinishedTaskForAllPasses() {

        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final UUID streamId_3 = randomUUID();

        when(eventRepository.getAllActiveStreamIds())
                .thenReturn(Stream.of(streamId_1, streamId_2))
                .thenReturn(Stream.of(streamId_3));

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


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenstreamsProcessedCountStepInfoIsNull() throws Exception {
        streamsProcessedCountStepInfo.set(startTransformation, null);
        startTransformation.go();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenstreamsProcessedCountStepInfoIsZero() throws Exception {
        streamsProcessedCountStepInfo.set(startTransformation, 0);
        startTransformation.go();
    }

    @Test
    public void shouldNotThrowExceptionWhenstreamsProcessedCountStepInfoIsNotZero() throws Exception {
        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final UUID streamId_3 = randomUUID();

        when(eventRepository.getAllActiveStreamIds())
                .thenReturn(Stream.of(streamId_1, streamId_2))
                .thenReturn(Stream.of(streamId_3));

        when(passesDeterminer.getPassValue()).thenReturn(1).thenReturn(2);
        when(passesDeterminer.isLastElementInPasses()).thenReturn(false).thenReturn(true);

        final Future future = mock(Future.class);
        final Future future2 = mock(Future.class);
        final Future future3 = mock(Future.class);
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(future).thenReturn(future2).thenReturn(future3);

        streamsProcessedCountStepInfo.set(startTransformation, 10);
        startTransformation.go();
    }

    @Test
    public void shouldNotLogOutputBasedOnStreamsProcessedCountStepInfoWhenStreamDoneIsZero() throws IllegalArgumentException, IllegalAccessException {
        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final UUID streamId_3 = randomUUID();

        when(eventRepository.getAllActiveStreamIds())
                .thenReturn(Stream.of(streamId_1, streamId_2))
                .thenReturn(Stream.of(streamId_3));

        when(passesDeterminer.getPassValue()).thenReturn(1).thenReturn(2);
        when(passesDeterminer.isLastElementInPasses()).thenReturn(false).thenReturn(true);

        final Future future = mock(Future.class);
        final Future future2 = mock(Future.class);
        final Future future3 = mock(Future.class);
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(future).thenReturn(future2).thenReturn(future3);

        streamsProcessedCountStepInfo.set(startTransformation, 10);
        startTransformation.go();
    }

    @Test
    public void shouldLogOutputBasedOnStreamsProcessedCountStepInfoWhenStreamDoneIsNotZero() throws IllegalArgumentException, IllegalAccessException {
        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final UUID streamId_3 = randomUUID();
        streamsDoneCount.set(startTransformation, new AtomicInteger(2));
        when(eventRepository.getAllActiveStreamIds())
                .thenReturn(Stream.of(streamId_1, streamId_2))
                .thenReturn(Stream.of(streamId_3));

        when(passesDeterminer.getPassValue()).thenReturn(1).thenReturn(2);
        when(passesDeterminer.isLastElementInPasses()).thenReturn(false).thenReturn(true);

        final Future future = mock(Future.class);
        final Future future2 = mock(Future.class);
        final Future future3 = mock(Future.class);
        when(executorService.submit(any(StreamTransformationTask.class))).thenReturn(future).thenReturn(future2).thenReturn(future3);
        when(stopWatch.getTime()).thenReturn(12222222l);
        streamsProcessedCountStepInfo.set(startTransformation, 2);
        startTransformation.go();
        verify(logger).info("Pass 1 - Streams count: 4 - time(ms): 12222222");
    }
}
