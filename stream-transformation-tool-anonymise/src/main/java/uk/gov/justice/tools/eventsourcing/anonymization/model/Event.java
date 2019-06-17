package uk.gov.justice.tools.eventsourcing.anonymization.model;


import java.util.List;

public class Event {

    private String eventName;

    private List<String> fieldsToBeIgnored;

    public Event() {
    }

    public Event(String eventName, List<String> fieldsToBeIgnored) {
        this.eventName = eventName;
        this.fieldsToBeIgnored = fieldsToBeIgnored;
    }


    public String getEventName() {
        return eventName;
    }

    public List<String> getFieldsToBeIgnored() {
        return fieldsToBeIgnored;
    }
}
