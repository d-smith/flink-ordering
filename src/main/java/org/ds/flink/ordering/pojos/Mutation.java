package org.ds.flink.ordering.pojos;

public class Mutation {
    public String id;
    public String operation;
    public Long timeStamp;

    public Mutation() {
    }

    public Mutation(String id, String operation, Long timeStamp) {
        this.id = id;
        this.operation = operation;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Mutation{" +
                "id='" + id + '\'' +
                ", operation='" + operation + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
