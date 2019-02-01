package uk.gov.justice.framework.tools.transformation;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;

public class SwarmStarterUtil {

    private static final Logger LOGGER = getLogger(SwarmStarterUtil.class);
    private String EVENT_TOOL_JAR_LOCATION;
    private String STREAM_JAR_LOCATION;
    private String STAND_ALOND_DS_LOCATION;
    private String MAIN_PROCESS_FILE_PATH;

    public SwarmStarterUtil() throws IOException {
        getResources();
    }

    public void runCommand(final boolean enableRemoteDebugging, final long timeoutInSeconds) throws IOException {
        runCommand(enableRemoteDebugging, timeoutInSeconds, 5l, "2048Mb");
    }

    public void runCommand(final boolean enableRemoteDebugging, final long timeoutInSeconds, final long streamCountReportingInterval, final String memoryOptions) throws IOException {
        final String memoryParmeter = format("-DXmx=%s", memoryOptions);
        final String streamCountReportingIntervalParameter = format("-DstreamCountReportingInterval=%s", streamCountReportingInterval);

        final String command = format("java %s -jar -Dorg.wildfly.swarm.mainProcessFile=%s -Devent.transformation.jar=%s %s -c %s %s %s",
                debug(enableRemoteDebugging), MAIN_PROCESS_FILE_PATH, STREAM_JAR_LOCATION, EVENT_TOOL_JAR_LOCATION, STAND_ALOND_DS_LOCATION,
                streamCountReportingIntervalParameter, memoryParmeter);

        startWildfly(timeoutInSeconds, command);
    }

    private String debug(boolean enableRemoteDebugging) {
        String debug = "";
        if (enableRemoteDebugging) {
            debug = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005";
        }
        return debug;
    }

    private void startWildfly(long timeoutInSeconds, String command) throws IOException {
        final Process exec = execute(command);
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(exec.getInputStream()));

        String line;
        while ((line = reader.readLine()) != null) {
            LOGGER.info(line);
        }

        LOGGER.info("Wildfly started successfully. Running transformation...");
        waitUntilDone(exec, timeoutInSeconds);
    }

    private String getResource(final String pattern) {

        final File dir = new File(this.getClass().getClassLoader().getResource("").getPath());
        final FileFilter fileFilter = new WildcardFileFilter(pattern);
        return dir.listFiles(fileFilter)[0].getAbsolutePath();
    }

    private boolean waitUntilDone(final Process exec, final long timeoutInSeconds) {
        boolean processTerminated = waitForProcessTermination(exec, timeoutInSeconds);

        if (!processTerminated) {
            killWIldfly(exec, timeoutInSeconds);
        } else {
            LOGGER.info("WildFly completed successfully.");
            return true;
        }
        return false;
    }

    @SuppressWarnings({"squid:S2629"})
    private void killWIldfly(final Process exec, final long timeoutInSeconds) {
        final boolean processTerminated;
        LOGGER.error(format("WildFly Swarm process failed to terminate after %s seconds!", timeoutInSeconds));
        exec.destroyForcibly();

        processTerminated = waitForProcessTermination(exec, 10L);
        if (!processTerminated) {
            LOGGER.error("Failed to forcibly terminate WildFly Swarm process!");
        } else {
            LOGGER.error("WildFly Swarm process forcibly terminated.");
        }
    }

    private boolean waitForProcessTermination(final Process exec, final long timeout) {
        try {
            return exec.waitFor(timeout, SECONDS);
        } catch (final InterruptedException e) {
            currentThread().interrupt();
        }
        return false;
    }

    @SuppressWarnings({"squid:S4721"})
    private Process execute(final String command) {
        try {
            return Runtime.getRuntime().exec(command);
        } catch (final IOException e) {
            throw new SwarmStarterException(format("Failed to execute external process '%s'", command), e);
        }
    }

    private void getResources() throws IOException {
        EVENT_TOOL_JAR_LOCATION = getResource("event-tool*.jar");
        STREAM_JAR_LOCATION = getResource("stream-transformations*.jar");
        STAND_ALOND_DS_LOCATION = getResource("standalone-ds.xml");
        MAIN_PROCESS_FILE_PATH = Paths.get(File.createTempFile("mainProcessFile", "tmp").toURI()).toAbsolutePath().toString();
    }
}
