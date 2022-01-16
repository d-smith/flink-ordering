# Flink-ordering 

Project to look at event ordering and any influence of processing time vs event time, really drives
home how timestamps are used to assign events to windows and reason about things
wrt windows.

Set up - create a kinesis stream, e.g.

```
aws kinesis create-stream --stream-name state-stream --shard-count 1
```

When running the producer and the jobs, set AWS_REGION and AWS_PROFILE environment vars.