package uk.gov.justice.tools.eventsourcing.transformation.service;

import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;

@FunctionalInterface
public interface StreamOperationRetryable {

    void execute() throws EventStreamException;
}


