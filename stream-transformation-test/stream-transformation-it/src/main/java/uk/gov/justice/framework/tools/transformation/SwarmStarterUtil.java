package uk.gov.justice.framework.tools.transformation;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SwarmStarterUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SwarmStarterUtil.class);

    public void runCommand() throws IOException {

        final String command = createCommandToExecuteTransformationTool();
        final Process exec = Runtime.getRuntime().exec(command);
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(exec.getInputStream()));

        String line = "";
        while ((line = reader.readLine()) != null) {
            LOGGER.info(line);
        }
    }

    private String createCommandToExecuteTransformationTool() throws IOException {
        final String eventToolJarLocation = getResource("event-tool*.jar");
        final String streamJarLocation = getResource("stream-transformations*.jar");
        final String standaloneDSLocation = getResource("standalone-ds.xml");
        final String mainProcessFilePath = Paths.get(File.createTempFile("mainProcessFile", "tmp").toURI()).toAbsolutePath().toString();

        return commandFrom(mainProcessFilePath, streamJarLocation, eventToolJarLocation, standaloneDSLocation);
    }

    private String commandFrom(final String mainProcessFilePath,
                               final String streamJarLocation,
                               final String eventToolJarLocation,
                               final String standaloneDSLocation) {
        return format("java -jar -Dorg.wildfly.swarm.mainProcessFile=%s -Devent.transformation.jar=%s %s -c %s",
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
}
