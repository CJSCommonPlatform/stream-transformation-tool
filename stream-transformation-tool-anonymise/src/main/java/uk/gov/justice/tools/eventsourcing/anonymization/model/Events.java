package uk.gov.justice.tools.eventsourcing.anonymization.model;


import java.util.List;

public class Events {

    private List<String> globalAttributes;

    private List<Event> events;

    public Events() {
    }

    public List<String> getGlobalAttributes() {
        return globalAttributes;
    }

    public List<Event> getEvents() {
        return events;
    }
}
