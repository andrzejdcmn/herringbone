package com.stripe.herringbone.test

import com.stripe.herringbone.flatten._
import org.apache.parquet.example.Paper
import org.apache.parquet.io.api.Binary
import org.scalatest._

class FlattenJobTest extends FlatSpec with Matchers {
  def toBinary(x: Array[Byte]) = Binary.fromByteArray(x)

  "truncate" should "truncate to correct length" in {
    val consumer = new FlatConsumer(Paper.r1, "__", false)
    val bytes = toBinary(Array[Byte](1,2,3,4))
    assert(consumer.truncate(bytes, 3).getBytes().sameElements(Array[Byte](1,2,3)))
  }

  "truncate" should "not truncate if unnecessary" in {
    val consumer = new FlatConsumer(Paper.r1, "__", false)
    val bytes = toBinary(Array[Byte](1,2,3,4))
    assert(consumer.truncate(bytes, 8) == bytes)
  }
}
