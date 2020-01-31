# kcl-akka-stream
Akka Streaming Source backed by Kinesis Client Library (KCL 2.x).

This library combines the convenience of Akka Streams with KCL 2.x checkpoint management, failover, load-balancing,
and re-sharding capabilities.

This library is thoroughly tested and currently used in production.


## Installation

```
resolvers in ThisBuild += Resolver.bintrayRepo("streetcontxt", "maven")
libraryDependencies += "com.streetcontxt" %% "kcl-akka-stream" % "2.1.0"
```


## Usage

Here are two simple examples on how to initialize the Kinesis consumer and listen for string messages.

The first example shows how to process Kinesis records in at-least-once fashion:
```scala
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.contxt.kinesis.{ConsumerConfig, KinesisSource}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    val consumerConfig = ConsumerConfig("myStream", "atLeastOnceApp")

    case class KeyMessage(key: String, message: String, markProcessed: () => Unit)

    val atLeastOnceSource = KinesisSource(consumerConfig)
      .map { kinesisRecord =>
        KeyMessage(kinesisRecord.partitionKey, kinesisRecord.data.utf8String, kinesisRecord.markProcessed)
      }
      // Records may be processed out of order without affecting checkpointing.
      .grouped(10)
      .map(batch => Random.shuffle(batch))
      .mapConcat(identity)
      .map { message =>
        // After a record is marked as processed, it is eligible to be checkpointed in DynamoDb.
        message.markProcessed()
        message
      }

    implicit val system = ActorSystem("Main")
    atLeastOnceSource.runWith(Sink.foreach(println))

    Thread.sleep(10.seconds.toMillis)
    Await.result(system.terminate(), Duration.Inf)
  }
}
```

The second examples shows how to implement no-guarantees processing:
```scala
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.contxt.kinesis.{ConsumerConfig, KinesisSource}

import scala.concurrent.Await
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    val consumerConfig = ConsumerConfig("myStream", "noGuaranteesApp")

    case class KeyMessage(key: String, message: String)

    val noGuaranteesSource = KinesisSource(consumerConfig)
      .map { kinesisRecord =>
        /* Every record must be marked as processed to allow stream state to be checkpointed in
         * DynamoDb. Failure to mark at least one record as processed will cause the application
         * to run out of memory. */
        kinesisRecord.markProcessed()
        kinesisRecord
      }
      .map { kinesisRecord =>
        KeyMessage(kinesisRecord.partitionKey, kinesisRecord.data.utf8String)
      }

    implicit val system = ActorSystem("Main")
    noGuaranteesSource.runWith(Sink.foreach(println))

    Thread.sleep(10.seconds.toMillis)
    Await.result(system.terminate(), Duration.Inf)
  }
}
```

Notice that each Kinesis record must be eventually marked as processed in both at-least-once and
no-guarantees scenarios. This is due to how Kinesis checkpointing is implemented.

A shard in a Kinesis stream is an ordered sequence of records. The shard is checkpointed by storing an offset
of the last processed record. However, if a record is not processed (for example, because of an exception),
then no further records after it can be checkpointed.

KinesisSource keeps track of all the uncheckpointed records and their ordering. This means you can process
records out of order in an asynchronous fashion. Each record must be eventually marked as processed by
calling `markProcessed()`, or the steam must be terminated with an exception. If the stream continues
after failing to process a record, and not marking it as processed, then no further records can be checkpointed,
eventually causing the system to run out of memory.

## Consumer Configuration
The Kinesis Consumer `ConsumerConfig` can be configured via HOCON configuration, which is common for Akka projects
```hocon
consumer {
  application-name = "test-app" # name of the application (consumer group)
  stream-name = "test-stream" # name of the stream to connect to

  position {
    initial = "latest" # (latest, trim-horizon, at-timestamp). defaults to latest
    time = "" # Only set if position is at-timestamp. Supports a valid Java Date parseable datetime string
  }
}
```

Then configuring the consumer using `ConsumerConfig.fromConfig`.
```scala
ConsumerConfig.fromConfig(system.settings.config.getConfig("consumer"))
```

The `ConsumerConfig` class also has methods for accepting raw AWS SDK clients which can be configured. 
If you require very custom configuration, this option is available.

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
