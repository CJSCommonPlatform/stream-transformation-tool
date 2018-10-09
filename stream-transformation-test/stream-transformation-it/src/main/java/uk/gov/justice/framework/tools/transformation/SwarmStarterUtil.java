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

    public void runCommand(final boolean enableRemoteDebugging, final long timeoutInSeconds) throws IOException {

        final String command = createCommandToExecuteTransformationTool(enableRemoteDebugging);
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

    private String createCommandToExecuteTransformationTool(final boolean enableRemoteDebugging) throws IOException {
        final String eventToolJarLocation = getResource("event-tool*.jar");
        final String streamJarLocation = getResource("stream-transformations*.jar");
        final String standaloneDSLocation = getResource("standalone-ds.xml");
        final String mainProcessFilePath = Paths.get(File.createTempFile("mainProcessFile", "tmp").toURI()).toAbsolutePath().toString();

        String debug = "";

        if (enableRemoteDebugging) {
            debug = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005";
        }

        return commandFrom(debug, mainProcessFilePath, streamJarLocation, eventToolJarLocation, standaloneDSLocation);
    }

    private String commandFrom(final String debug,
                               final String mainProcessFilePath,
                               final String streamJarLocation,
                               final String eventToolJarLocation,
                               final String standaloneDSLocation) {
        return format("java %s -jar -Dorg.wildfly.swarm.mainProcessFile=%s -Devent.transformation.jar=%s %s -c %s",
                debug,
                mainProcessFilePath,
                streamJarLocation,
                eventToolJarLocation,
                standaloneDSLocation);
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
}
