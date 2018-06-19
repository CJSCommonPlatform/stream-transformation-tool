package uk.gov.justice.tools.eventsourcing.transformation.api;

import java.util.Objects;

/**
 * Identifies possible actions to be taken on a stream
 */
@SuppressWarnings("WeakerAccess")
public class Action {

    /**
     * Action specifying a stream to be transformed.
     * A backup of the stream will be created as part of the transform action
     */
    public static final Action TRANSFORM = new Action(true, false, true);

    /**
     * Action to deactivate a stream. No backup of the stream will be created
     */
    public static final Action DEACTIVATE = new Action(false, true, false);

    /**
     * Action to archive/deactivate a stream. Retained for backward compatibility. No backup of the stream will be created
     * @deprecated use DEACTIVATE instead
     */
    @Deprecated
    public static final Action ARCHIVE = new Action(false, true, false);

    /**
     * No action to be taken on the stream.
     */
    public static final Action NO_ACTION = new Action(false, false, false);

    private final boolean transformStream;
    private final boolean deactivateStream;
    private final boolean keepBackup;

    public Action(final boolean transformStream, final boolean deactivateStream, final boolean keepBackup) {
        this.transformStream = transformStream;
        this.deactivateStream = deactivateStream;
        this.keepBackup = keepBackup;
    }

    public boolean isTransform() {
        return transformStream;
    }

    public boolean isDeactivate() {
        return deactivateStream;
    }

    public boolean isKeepBackup() {
        return keepBackup;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Action)) {
            return false;
        }
        final Action that = (Action) o;
        return this.transformStream == that.transformStream &&
                this.deactivateStream == that.deactivateStream &&
                this.keepBackup == that.keepBackup;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.transformStream, this.deactivateStream, this.keepBackup);
    }
}
