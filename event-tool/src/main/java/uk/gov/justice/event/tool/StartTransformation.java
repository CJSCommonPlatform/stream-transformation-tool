package uk.gov.justice.event.tool;

import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static org.wildfly.swarm.bootstrap.Main.MAIN_PROCESS_FILE;

import uk.gov.justice.event.tool.task.StreamTransformationTask;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStream;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationService;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.concurrent.ManagedTaskListener;
import javax.inject.Inject;

import org.slf4j.Logger;

@Singleton
@Startup
public class StartTransformation implements ManagedTaskListener {

    private static final String NO_PROCESS_FILE_WARNING = "!!!!! No Swarm Process File specific, application will not auto-shutdown on completion. Please use option '-Dorg.wildfly.swarm.mainProcessFile=/pathTo/aFile' to specify location of process file with read/write permissions !!!!!";

    @Inject
    private Logger logger;

    @Resource(name = "event-tool")
    private ManagedExecutorService executorService;

    @Inject
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @Inject
    private EventStreamTransformationService eventStreamTransformationService;

    @Inject
    private PassesDeterminer passesDeterminer;

    Deque<Future<UUID>> outstandingTasks = new LinkedBlockingDeque<>();

    boolean allTasksCreated = false;

    @PostConstruct
    void go() {
        logger.info("-------------- Invoke Event Streams Transformation -------------");

        checkForMainProcessFile();

        createTransformationTasks(passesDeterminer.getPassValue());

        logger.info("-------------- Invocation of Event Streams Transformation Completed --------------");
    }

    private void createTransformationTasks(final int pass) {
        final Stream<UUID> activeStreams = eventStreamJdbcRepository.findActive().map(EventStream::getStreamId);

        activeStreams
                .forEach(streamId -> {
                    final StreamTransformationTask transformationTask = new StreamTransformationTask(streamId, eventStreamTransformationService, this, pass);
                    outstandingTasks.add(executorService.submit(transformationTask));
                });
        activeStreams.close();

        if (outstandingTasks.isEmpty()) {
            shutdown();
        }

        allTasksCreated = true;
    }

    public void taskStarting(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task) {
        logger.debug("Starting Transformation task");
    }

    public void taskSubmitted(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task) {
        logger.debug("Submitted Transformation task");
    }

    public void taskDone(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task, final Throwable throwable) {
        logger.debug("Completed Transformation task");
        removeOutstandingTask(futureTask);
        nextPassIfFinished();
    }

    public void taskAborted(final Future<?> futureTask, final ManagedExecutorService managedExecutorService, final Object task, final Throwable throwable) {
        logger.error(String.format("Aborted Transformation task: '%s'", throwable.getMessage()));
        removeOutstandingTask(futureTask);
        shutDownIfFinished();
    }

    private void removeOutstandingTask(final Future<?> futureTask) {
        outstandingTasks.remove(futureTask);
    }

    private void nextPassIfFinished() {
        if (isTaskFinished()) {
            final boolean isLastElementInPasses = passesDeterminer.isLastElementInPasses();
            if (isLastElementInPasses) {
                shutdown();
            } else {
                createTransformationTasks(passesDeterminer.getNextPassValue());
            }
        }
    }

    private void shutDownIfFinished() {
        if (isTaskFinished()) {
            shutdown();
        }
    }

    private boolean isTaskFinished() {
        return allTasksCreated && outstandingTasks.isEmpty();
    }

    private void shutdown() {
        logger.info("========== ALL TASKS HAVE BEEN DISPATCHED -- ATTEMPTING SHUTDOWN =================");

        final String processFile = System.getProperty(MAIN_PROCESS_FILE);
        if (processFile != null) {
            final File uuidFile = new File(processFile);
            if (uuidFile.exists()) {
                try {
                    delete(uuidFile.toPath());
                } catch (IOException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(format("Failed to delete process file '%s', file does not exist", processFile), e);
                    }
                }
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn(format("Failed to delete process file '%s', file does not exist", processFile));
                }
            }
        }
    }

    private void checkForMainProcessFile() {
        if (System.getProperty(MAIN_PROCESS_FILE) == null) {
            logger.warn(NO_PROCESS_FILE_WARNING);
        }
    }

}