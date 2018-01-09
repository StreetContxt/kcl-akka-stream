package com.contxt.kinesis

import org.scalatest.{ Matchers, WordSpec }

class MessageUtilTest extends WordSpec with Matchers {

  "MessageUtil" when {
    "grouping by key" should {
      "keep duplicates" in {
        MessageUtil.groupByKey(IndexedSeq(
          "k1" -> "m1",
          "k2" -> "m1",
          "k2" -> "m1",
          "k1" -> "m2"
        )) shouldEqual Map(
          "k1" -> IndexedSeq("m1", "m2"),
          "k2" -> IndexedSeq("m1", "m1")
        )
      }
    }

    "removing reprocessed messages" should {
      "keep the original sequence when there is no duplication" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m2", "m3"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3"
        )
      }

      "detect replay mismatch in the beginning" in {
        an[Exception] should be thrownBy{
          MessageUtil.removeReprocessed(IndexedSeq(
            "m1", "m2", "m1", "m3"
          ))
        }
      }

      "detect replay mismatch in the middle" in {
        an[Exception] should be thrownBy{
          MessageUtil.removeReprocessed(IndexedSeq(
            "m1", "m2", "m3", "m2", "m4", "m5"
          ))
        }
      }

      "detect replay mismatch at the end" in {
        an[Exception] should be thrownBy{
          MessageUtil.removeReprocessed(IndexedSeq(
            "m1", "m2", "m3", "m2", "m4"
          ))
        }
      }

      "detect reordering of messages in the beginning" in {
        an[Exception] should be thrownBy{
          MessageUtil.removeReprocessed(IndexedSeq(
            "m1", "m2", "m2", "m1", "m3"
          ))
        }
      }

      "detect reordering of messages in the middle" in {
        an[Exception] should be thrownBy{
          MessageUtil.removeReprocessed(IndexedSeq(
            "m1", "m2", "m3", "m3", "m2", "m4"
          ))
        }
      }

      "detect reordering of messages at the end" in {
        an[Exception] should be thrownBy{
          MessageUtil.removeReprocessed(IndexedSeq(
            "m1", "m2", "m3", "m3", "m2"
          ))
        }
      }
    }

    "removing single reprocessed message" should {
      "handle repeated leading message" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m1", "m1", "m2", "m3"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3"
        )
      }

      "handle repeated message in the middle" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m2", "m2", "m2", "m3"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3"
        )
      }

      "handle repeated trailing message" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m2", "m3", "m3", "m3"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3"
        )
      }
    }

    "removing a sequence of reprocessed messages" should {
      "handle repeated leading sequence" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m2", "m1", "m2", "m3"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3"
        )
      }

      "handle repeated sequence in the middle" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m2", "m3", "m2", "m3", "m4"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3", "m4"
        )
      }

      "handle repeated trailing sequence" in {
        MessageUtil.removeReprocessed(IndexedSeq(
          "m1", "m2", "m3", "m2", "m3"
        )) shouldEqual IndexedSeq(
          "m1", "m2", "m3"
        )
      }
    }

    "handle repeated retry sequences" in {
      MessageUtil.removeReprocessed(IndexedSeq(
        "m1", "m2", "m3", "m2", "m2", "m3", "m4"
      )) shouldEqual IndexedSeq(
        "m1", "m2", "m3", "m4"
      )
    }
  }
}
