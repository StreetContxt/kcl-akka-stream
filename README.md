# kcl-akka-stream
Akka Streaming Source backed by Kinesis Client Library (KCL).

This library combines the convenience of Akka Streams with KCL checkpoint management, failover, load-balancing,
and re-sharding capabilities.

This library is thoroughly tested, but still in early stages.


## Amazon Licensing Restrictions
**KCL license is not compatible with open source licenses!** See
[this discussion](https://issues.apache.org/jira/browse/LEGAL-198) for more details.

As such, the licensing terms of this library is Apache 2 license **PLUS** whatever restrictions
are imposed by the KCL license.


## No Message Ordering
Kinesis consumer **does not guarantee mutually exclusive processing of shards** during failover or load-balancing.
See [Kinesis Troubleshooting Guide](http://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html)
for more details.

Kinesis producer library **does not provide message ordering guarantees** at a reasonable throughput,
see [this ticket](https://github.com/awslabs/amazon-kinesis-producer/issues/23) for more details.


## Integration Tests
To run integration tests:
* Setup local AWS credentials (for example, via `~/.aws/credentials` file)
* Set `KINESIS_TEST_REGION` environmental variable
* Run `sbt it:test`
* Cancelled tests will leave temporary Kinesis streams and DynamoDb tables prefixed with `deleteMe_`
