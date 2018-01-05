package uk.gov.justice.event.tool;

import uk.gov.justice.event.tool.task.StreamTransformationTask;
import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcEventRepository;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationService;

import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.concurrent.ManagedTaskListener;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class StartTransformation implements ManagedTaskListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartTransformation.class);

    @Resource // (name = "event-tool")
    private ManagedExecutorService executorService;

    @Inject
    private JdbcEventRepository jdbcEventRepository;

    @Inject
    private EventStreamTransformationService eventStreamTransformationService;

    Deque<Future<UUID>> outstandingTasks = new LinkedBlockingDeque<>();

    boolean allTasksCreated = false;

    @PostConstruct
    void go() {
        LOGGER.info("-------------- Invoke Event Streams Transformation -------------!");

        jdbcEventRepository.getStreamOfAllEventStreams()
                .forEach(stream -> {
                    final StreamTransformationTask transformationTask = new StreamTransformationTask(stream, eventStreamTransformationService, this);
                    outstandingTasks.add(executorService.submit(transformationTask));
                });

        allTasksCreated = true;

        if (outstandingTasks.isEmpty()) {
            shutdown();
        }
        LOGGER.info("-------------- Invocation of Event Streams Transformation Completed --------------");
    }

    public void taskStarting(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task) {
        LOGGER.debug("Starting Transformation task");
    }

    public void taskSubmitted(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task) {
        LOGGER.debug("Submitted Transformation task");
    }

    public void taskDone(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task, final Throwable throwable) {
        LOGGER.debug("Completed Transformation task");
        removeOutstandingTask(futureTask);
        shutdownIfFinished();
    }

    public void taskAborted(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task, final Throwable throwable) {
        LOGGER.debug("Aborted Transformation task");
        removeOutstandingTask(futureTask);
        shutdownIfFinished();
    }

    private void removeOutstandingTask(final Future<?> futureTask) {
        outstandingTasks.remove(futureTask);
    }

    private void shutdownIfFinished() {
        if (allTasksCreated && outstandingTasks.isEmpty()) {
            shutdown();
        }
    }

    private void shutdown() {
        LOGGER.info("========== ALL TASKS HAVE BEEN DISPATCHED -- SHUTDOWN =================");
        // TODO add file hook that triggers actual shutdown
    }
}