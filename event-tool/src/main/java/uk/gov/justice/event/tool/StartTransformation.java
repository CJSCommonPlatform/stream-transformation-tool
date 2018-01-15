package uk.gov.justice.event.tool;

import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static org.wildfly.swarm.bootstrap.Main.MAIN_PROCESS_FILE;

import uk.gov.justice.event.tool.task.StreamTransformationTask;
import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcEventRepository;
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
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class StartTransformation implements ManagedTaskListener {

    private static final String NO_PROCESS_FILE_WARNING = "!!!!! No Swarm Process File specific, application will not auto-shutdown on completion. Please use option '-Dorg.wildfly.swarm.mainProcessFile=/pathTo/aFile' to specify location of process file with read/write permissions !!!!!";

    private static final Logger LOGGER = LoggerFactory.getLogger(StartTransformation.class);

    @Resource(name = "event-tool")
    private ManagedExecutorService executorService;

    @Inject
    private JdbcEventRepository jdbcEventRepository;

    @Inject
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @Inject
    private EventStreamTransformationService eventStreamTransformationService;

    Deque<Future<UUID>> outstandingTasks = new LinkedBlockingDeque<>();

    boolean allTasksCreated = false;

    @PostConstruct
    void go() {
        LOGGER.info("-------------- Invoke Event Streams Transformation -------------");

        checkForMainProcessFile();

        createTransformationTasks();

        shutdownIfFinished();

        LOGGER.info("-------------- Invocation of Event Streams Transformation Completed --------------");
    }

    private void createTransformationTasks() {
        final Stream<UUID> activeStreams = eventStreamJdbcRepository.findActive().map(EventStream::getStreamId);
        activeStreams
                .forEach(streamId -> {
                    final StreamTransformationTask transformationTask = new StreamTransformationTask(streamId, eventStreamTransformationService, this);
                    outstandingTasks.add(executorService.submit(transformationTask));
                });
        activeStreams.close();

        allTasksCreated = true;
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
        LOGGER.info("========== ALL TASKS HAVE BEEN DISPATCHED -- ATTEMPTING SHUTDOWN =================");

        final String processFile = System.getProperty(MAIN_PROCESS_FILE);
        if (processFile != null) {
            final File uuidFile = new File(processFile);
            if (uuidFile.exists()) {
                try {
                    delete(uuidFile.toPath());
                } catch (IOException e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(format("Failed to delete process file '%s', file does not exist", processFile), e);
                    }
                }
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Failed to delete process file '%s', file does not exist", processFile));
                }
            }
        }
    }

    private void checkForMainProcessFile() {
        if (System.getProperty(MAIN_PROCESS_FILE) == null) {
            LOGGER.warn(NO_PROCESS_FILE_WARNING);
        }
    }
}