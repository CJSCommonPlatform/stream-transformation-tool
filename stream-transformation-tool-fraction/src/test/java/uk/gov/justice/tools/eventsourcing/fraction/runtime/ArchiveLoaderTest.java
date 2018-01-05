package uk.gov.justice.tools.eventsourcing.fraction.runtime;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArchiveLoaderTest {

    @InjectMocks
    private ArchiveLoader archiveLoader;

    private Field libraryField;

    @Before
    public void setup() throws Exception {
        libraryField = archiveLoader.getClass().getDeclaredField("library");
        libraryField.setAccessible(true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenLibraryNameIsEmpty() throws Exception {
        libraryField.set(archiveLoader, "");
        archiveLoader.process();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenLibraryNameIsNull() throws Exception {
        libraryField.set(archiveLoader, null);
        archiveLoader.process();
    }
}
