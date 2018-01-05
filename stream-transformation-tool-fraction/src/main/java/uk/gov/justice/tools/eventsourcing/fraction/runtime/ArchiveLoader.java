package uk.gov.justice.tools.eventsourcing.fraction.runtime;

import static java.lang.String.format;
import static org.jboss.shrinkwrap.api.ShrinkWrap.createFromZipFile;

import java.nio.file.Paths;

import javax.inject.Inject;

import org.jboss.logging.Logger;
import org.jboss.shrinkwrap.api.Archive;
import org.wildfly.swarm.spi.api.DeploymentProcessor;
import org.wildfly.swarm.spi.api.JARArchive;
import org.wildfly.swarm.spi.runtime.annotations.ConfigurationValue;
import org.wildfly.swarm.spi.runtime.annotations.DeploymentScoped;
import org.wildfly.swarm.undertow.WARArchive;


@DeploymentScoped
public class ArchiveLoader implements DeploymentProcessor {

    private final Logger LOGGER = Logger.getLogger(ArchiveLoader.class);

    private final Archive<?> archive;

    @Inject
    @ConfigurationValue("event.transformation.jar")
    private String library;

    @Inject
    public ArchiveLoader(final Archive archive) {
        this.archive = archive;
    }

    @Override
    public void process() throws Exception {

        if (library == null || library.isEmpty()) {
            throw new IllegalArgumentException("Invalid event.transformation.jar argument");
        }

        LOGGER.info(format("Merging transformation classes from %s", library));

        final JARArchive webArchive = createFromZipFile(JARArchive.class, Paths.get(library).toFile());
        archive.as(WARArchive.class).merge(webArchive, "WEB-INF/classes");
    }
}