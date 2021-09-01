package com.contxt.kinesis

object MessageUtil {

  /** Requires unique messages that were sent at least once and then processed at least once. Sending a message batch
    * can be retried, as long as the order of messages remains the same. Processing of messages can restart at an
    * earlier checkpoint, as long as the order of messages remains the same.
    */
  def dedupAndGroupByKey(keyMessagePairs: Seq[(String, String)]): Map[String, IndexedSeq[String]] = {
    groupByKey(keyMessagePairs).map {
      case (key, value) => key -> removeReprocessed(key, value)
    }
  }

  def groupByKey(keyMessagePairs: Seq[(String, String)]): Map[String, IndexedSeq[String]] = {
    keyMessagePairs.toIndexedSeq
      .groupBy { case (key, _) => key }
      .view
      .mapValues { keysWithMessages =>
        keysWithMessages.map { case (_, message) => message }
      }
  }.toMap

  private[kinesis] def removeReprocessed(key: String, messages: IndexedSeq[String]): IndexedSeq[String] = {
    def unwindRetry(sliceCandidate: IndexedSeq[String], from: Int): Int = {
      var i = 0
      while (from + i < messages.size && i < sliceCandidate.size && sliceCandidate(i) == messages(from + i)) i += 1
      i
    }
    def unwindRetries(sliceCandidate: IndexedSeq[String], from: Int): Int = {
      var j = from
      var advanced = 0
      do {
        advanced = unwindRetry(sliceCandidate, j)
        j += advanced
      } while (advanced > 0)
      j
    }
    val distinct = messages.distinct
    var (i, j) = (0, 0)
    var lastRestartedAt = 0
    while (j < messages.size) {
      val lastDistinct = distinct.lift(i)
      val lastMessage = messages(j)
      if (lastDistinct.isEmpty || lastDistinct.get != lastMessage) {
        val restartedAt = distinct.lastIndexOf(lastMessage)
        if (restartedAt < lastRestartedAt) throw new UnexpectedMessageSequence(key, lastMessage, messages)
        lastRestartedAt = restartedAt
        val reprocessedSliceCandidate = distinct.slice(restartedAt, i)
        val lastIndexOfRetrySequence = unwindRetries(reprocessedSliceCandidate, j) - 1
        if (lastIndexOfRetrySequence < j || reprocessedSliceCandidate.last != messages(lastIndexOfRetrySequence)) {
          throw new UnexpectedMessageSequence(key, lastMessage, messages)
        }
        j = lastIndexOfRetrySequence + 1
      } else {
        i += 1
        j += 1
      }
    }
    distinct
  }

  private class UnexpectedMessageSequence(key: String, lastMessage: String, messages: IndexedSeq[String])
      extends Exception(s"Messages for key `$key` starting from `$lastMessage` were processed out of order: ${messages
        .mkString(",")}")
}
