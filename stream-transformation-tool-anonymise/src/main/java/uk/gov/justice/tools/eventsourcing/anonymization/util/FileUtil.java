package uk.gov.justice.tools.eventsourcing.anonymization.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public final class FileUtil {

    private FileUtil() {
    }

    public static String getFileContentsAsString(final String fileName) {
        final StringBuilder sb = new StringBuilder();
        getBufferedReader(getReader(getResourceAsStream(fileName))).lines().forEach(sb::append);
        return sb.toString();
    }

    public static BufferedReader getBufferedReader(final InputStreamReader inputStreamReader) {
        return new BufferedReader(inputStreamReader);
    }

    public static InputStreamReader getReader(final InputStream fileStream) {
        return new InputStreamReader(fileStream);
    }

    public static InputStream getResourceAsStream(final String fileName) {
        return FileUtil.class.getClassLoader().getResourceAsStream(fileName);
    }

    public static List<String> getFileContentsAsList(final String fileName) {
        return getBufferedReader(getReader(getResourceAsStream(fileName))).lines().collect(Collectors.toList());
    }

    public static List<String> getFilesFromResourceDirectory(final String path) {
        return getFileContentsAsList(path);
    }

}
