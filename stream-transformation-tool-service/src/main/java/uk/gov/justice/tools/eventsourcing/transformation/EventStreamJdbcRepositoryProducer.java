package uk.gov.justice.tools.eventsourcing.transformation;

import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepositoryFactory;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

@ApplicationScoped
public class EventStreamJdbcRepositoryProducer {

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Inject
    private EventStreamJdbcRepositoryFactory eventStreamJdbcRepositoryFactory;

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Produces
    public EventStreamJdbcRepository eventStreamJdbcRepository() {

        final EventSourceDefinition defaultEventSourceDefinition = eventSourceDefinitionRegistry.getDefaultEventSourceDefinition();

        final Optional<String> dataSource = defaultEventSourceDefinition.getLocation().getDataSource();

        return eventStreamJdbcRepositoryFactory.eventStreamJdbcRepository(dataSource.get());
    }
}
