/*
 * Copyright (C) 2010 Mikhail Vorozhtsov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mvv.layson.bson

import java.util.Date

sealed trait BsonValue extends NotNull {
  def code: Int
  def size: Int
  def serialize: Iterator[Byte]
}
sealed trait OptBsonValue extends BsonValue
sealed trait MandatoryBsonValue extends BsonValue

sealed trait OptSimpleBsonValue extends OptBsonValue
sealed trait SimpleBsonValue extends OptSimpleBsonValue with MandatoryBsonValue

sealed trait OptBsonBool extends OptSimpleBsonValue
object OptBsonBool {
  implicit def optBsonBoolToBoolean(x: OptBsonBool) = x match {
    case BsonNull => null
    case BsonBool(x) => java.lang.Boolean.valueOf(x)
  }
}
sealed trait BsonBool extends OptBsonBool with SimpleBsonValue {
  val value: Boolean
  def code = 0x08
  def size = 1
  def serialize = Iterator.single((if (value) 1 else 0).byteValue)
}
object BsonBool {
  object True extends BsonBool {
    val value = true
  }
  object False extends BsonBool {
    val value = false
  }

  def apply(value: Boolean) = if (value) True else False
  def unapply(x: BsonBool): Option[Boolean] = Some(x.value)

  implicit def bsonBoolToBoolean(x: BsonBool) = x.value
}

sealed trait OptNumericBsonValue extends OptSimpleBsonValue
sealed trait NumericBsonValue extends OptNumericBsonValue
                                 with SimpleBsonValue

sealed trait OptIntegralBsonValue extends OptNumericBsonValue
sealed trait IntegralBsonValue extends OptIntegralBsonValue
                                  with NumericBsonValue

sealed trait OptBsonInt extends OptIntegralBsonValue
object OptBsonInt {
  implicit def optBsonIntToByte(x: OptBsonInt) = x match {
    case BsonNull => null
    case BsonInt(x) => java.lang.Byte.valueOf(x.byteValue)
  }
  implicit def optBsonIntToShort(x: OptBsonInt) = x match {
    case BsonNull => null
    case BsonInt(x) => java.lang.Short.valueOf(x.shortValue)
  }
  implicit def optBsonIntToInt(x: OptBsonInt) = x match {
    case BsonNull => null
    case BsonInt(x) => java.lang.Integer.valueOf(x)
  }
}
final case class BsonInt(value: Int) extends OptBsonInt
                                        with IntegralBsonValue {
  def code = 0x10
  def size = 4
  def serialize = Iterator(((value >> 0) & 0xFF).byteValue,
                           ((value >> 8) & 0xFF).byteValue,
                           ((value >> 16) & 0xFF).byteValue,
                           ((value >> 24) & 0xFF).byteValue)
}
object BsonInt {
  val Zero = BsonInt(0)

  implicit def bsonIntToByte(x: BsonInt) = x.value.byteValue
  implicit def bsonIntToShort(x: BsonInt) = x.value.shortValue
  implicit def bsonIntToInt(x: BsonInt) = x.value
}

sealed trait OptBsonLong extends OptIntegralBsonValue
object OptBsonLong {
  implicit def byteToOptBsonLong(x: java.lang.Byte) =
    if (x == null) BsonNull else BsonLong(x.longValue)
  implicit def shortToOptBsonLong(x: java.lang.Short) =
    if (x == null) BsonNull else BsonLong(x.longValue)
  implicit def intToOptBsonLong(x: java.lang.Integer) =
    if (x == null) BsonNull else BsonLong(x.longValue)
  implicit def optBsonLongToByte(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => java.lang.Byte.valueOf(x.byteValue)
  }
  implicit def optBsonLongToShort(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => java.lang.Short.valueOf(x.shortValue)
  }
  implicit def optBsonLongToInt(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => java.lang.Integer.valueOf(x.intValue)
  }
  implicit def optBsonLongToLong(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => java.lang.Long.valueOf(x)
  }
}
final case class BsonLong(value: Long) extends OptBsonLong
                                          with IntegralBsonValue {
  def code = 0x11
  def size = 8
  def serialize = Iterator(((value >> 0) & 0xFF).byteValue,
                           ((value >> 8) & 0xFF).byteValue,
                           ((value >> 16) & 0xFF).byteValue,
                           ((value >> 24) & 0xFF).byteValue,
                           ((value >> 32) & 0xFF).byteValue,
                           ((value >> 40) & 0xFF).byteValue,
                           ((value >> 48) & 0xFF).byteValue,
                           ((value >> 56) & 0xFF).byteValue)
}
object BsonLong {
  val Zero = BsonLong(0)

  implicit def byteToBsonLong(x: Byte) = BsonLong(x)
  implicit def shortToBsonLong(x: Short) = BsonLong(x)
  implicit def intToBsonLong(x: Int) = BsonLong(x)
  implicit def bsonLongToByte(x: BsonLong) = x.value.byteValue
  implicit def bsonLongToShort(x: BsonLong) = x.value.shortValue
  implicit def bsonLongToInt(x: BsonLong) = x.value.intValue
  implicit def bsonLongToLong(x: BsonLong) = x.value
}

sealed trait OptBsonDouble extends OptNumericBsonValue
object OptBsonDouble {
  implicit def optBsonDoubleToFloat(x: OptBsonDouble) = x match {
    case BsonNull => null
    case BsonDouble(x) => java.lang.Float.valueOf(x.floatValue)
  }
  implicit def optBsonDoubleToDouble(x: OptBsonDouble) = x match {
    case BsonNull => null
    case BsonDouble(x) => java.lang.Double.valueOf(x)
  }
}
final case class BsonDouble(value: Double) extends OptBsonDouble
                                              with NumericBsonValue {
  def code = 0x01
  def size = 8
  def serialize =
    BsonLong(java.lang.Double.doubleToRawLongBits(value)).serialize
}
object BsonDouble {
  val Zero = BsonDouble(0.0)

  implicit def bsonDoubleToFloat(x: BsonDouble) = x.value.floatValue
  implicit def bsonDoubleToDouble(x: BsonDouble) = x.value
}

sealed trait OptBsonDate extends OptSimpleBsonValue
object OptBsonDate {
  implicit def optBsonDateToDate(x: OptBsonDate) = x match {
    case BsonNull => null
    case BsonDate(x) => x
  }
}
final case class BsonDate(value: Date) extends OptBsonDate
                                          with SimpleBsonValue {
  require(value != null)
  def code = 0x09
  def size = 8
  def serialize = BsonLong(value.getTime).serialize
}
object BsonDate {
  implicit def dateToBsonDate(x: Date) = BsonDate(x)
  implicit def bsonDateToDate(x: BsonDate) = x.value
}

sealed trait OptBsonId extends OptSimpleBsonValue
final case class BsonId(time: Int, machine: Int, increment: Int)
                   extends OptBsonId with SimpleBsonValue {
  def code = 0x07
  def size = 12
  def serialize = BsonInt(time).serialize ++
                  BsonInt(machine).serialize ++
                  BsonInt(increment).serialize
  override def toString =
    "%08x%08x%08x" format (time,
                           java.lang.Integer.reverseBytes(machine),
                           java.lang.Integer.reverseBytes(increment))
}
object BsonId {
  val Zero = BsonId(0, 0, 0)
}

sealed trait OptBsonStr extends OptSimpleBsonValue
object OptBsonStr {
  implicit def optBsonStrToString(x: OptBsonStr) = x match {
    case BsonNull => null
    case BsonStr(x) => x
  }
}
trait BsonStr extends OptBsonStr with SimpleBsonValue {
  def iterator: Iterator[Char]
  def value: String
  final def code = 2
  def size = 0
  final def serialize = Iterator.empty
}
class StrictBsonStr(val value: String) extends BsonStr {
  require(value != null)
  def iterator = value.iterator
}
class LazyBsonStr(it: Iterator[Char]) extends BsonStr {
  private lazy val chars = it.toStream
  def iterator = chars.iterator
  def value = chars.mkString
}
object BsonStr {
  val Empty = BsonStr("")

  def apply(value: String): BsonStr = new StrictBsonStr(value)
  def apply(it: Iterator[Char]): BsonStr = new LazyBsonStr(it)
  def unapply(x: BsonStr): Option[String] = Some(x.value)

  implicit def stringToBsonStr(x: String) = BsonStr(x)
  implicit def bsonStrToString(x: BsonStr) = x.value
}

trait OptCompoundBsonValue extends OptBsonValue
trait CompoundBsonValue extends OptCompoundBsonValue with MandatoryBsonValue

sealed trait OptBsonArray extends OptCompoundBsonValue
trait BsonArray extends OptBsonArray with CompoundBsonValue {
  def iterator: Iterator[BsonValue]
  def elements: Seq[BsonValue]
  final def code = 0x04
  def size = 0
  final def serialize = Iterator.empty
}
class StrictBsonArray(val elements: Seq[BsonValue]) extends BsonArray {
  def iterator = elements.iterator
}
class LazyBsonArray(it: Iterator[BsonValue]) extends BsonArray {
  private lazy val elems = it.toStream
  def iterator = elems.iterator
  def elements = elems
}
object BsonArray {
  val Empty = BsonArray()

  def apply(elem: BsonValue*) = new StrictBsonArray(elem)
  def apply(it: Iterator[BsonValue]) = new LazyBsonArray(it)
  def unapply(x: BsonArray): Option[Seq[BsonValue]] = Some(x.elements)
}

sealed trait OptBsonObject extends OptCompoundBsonValue
trait BsonObject extends OptBsonObject with CompoundBsonValue {
  def iterator: Iterator[(String, BsonValue)]
  def members: Seq[(String, BsonValue)]
  def membersMap: Map[String, BsonValue]
  def get(key: String): Option[BsonValue]
  final def code = 0x03
  def size = 0
  final def serialize = Iterator.empty
}
class SeqBsonObject(val members: Seq[(String, BsonValue)]) extends BsonObject {
  def iterator = members.iterator
  def membersMap = members.toMap
  def get(key: String) = members.find(_._1 == key).map(_._2)
}
class MapBsonObject(val membersMap: Map[String, BsonValue]) extends BsonObject {
  def iterator = membersMap.iterator
  def members = membersMap.toSeq
  def get(key: String) = membersMap.get(key)
}
class LazyBsonObject(it: Iterator[(String, BsonValue)]) extends BsonObject {
  lazy val members = it.toStream
  override def iterator = members.iterator
  def membersMap = members.toMap
  override def get(key: String) = members.find(_._1 == key).map(_._2)
}
object BsonObject {
  val Empty = BsonObject()

  def apply(members: (String, BsonValue)*) = new SeqBsonObject(members)
  def apply[T](m: (String, T))(implicit conv: T => BsonValue) =
    new SeqBsonObject(Seq(m._1 -> conv(m._2)))
  def apply(map: Map[String, BsonValue]) = new MapBsonObject(map)
  def apply(it: Iterator[(String, BsonValue)]) = new LazyBsonObject(it)
  def unapply(x: BsonObject): Option[Map[String, BsonValue]] =
    Option(x.membersMap)
}

object BsonNull extends OptBsonValue
                   with OptBsonBool
                   with OptBsonInt
                   with OptBsonLong
                   with OptBsonDouble
                   with OptBsonDate
                   with OptBsonStr
                   with OptBsonArray
                   with OptBsonObject {
  def code = 0x0A
  def size = 0
  def serialize = Iterator.empty
}

object BsonMinKey extends BsonValue {
  def code = 0xFF
  def size = 0
  def serialize = Iterator.empty
}

object BsonMaxKey extends BsonValue {
  def code = 0x7F
  def size = 0
  def serialize = Iterator.empty
}

object BsonValue {
  implicit def booleanToBsonBool(x: Boolean) = BsonBool(x)
  implicit def booleanToOptBsonBool(x: java.lang.Boolean) =
    if (x == null) BsonNull else BsonBool(x.booleanValue)
  implicit def byteToBsonInt(x: Byte) = BsonInt(x)
  implicit def byteToOptBsonInt(x: java.lang.Byte) =
    if (x == null) BsonNull else BsonInt(x.intValue)
  implicit def shortToBsonInt(x: Short) = BsonInt(x)
  implicit def shortToOptBsonInt(x: java.lang.Short) =
    if (x == null) BsonNull else BsonInt(x.intValue)
  implicit def intToBsonInt(x: Int) = BsonInt(x)
  implicit def intToOptBsonInt(x: java.lang.Integer) =
    if (x == null) BsonNull else BsonInt(x.intValue)
  implicit def longToBsonLong(x: Long) = BsonLong(x)
  implicit def longToOptBsonLong(x: java.lang.Long) =
    if (x == null) BsonNull else BsonLong(x.longValue)
  implicit def floatToBsonDouble(x: Float) = BsonDouble(x)
  implicit def floatToOptBsonDouble(x: java.lang.Float) =
    if (x == null) BsonNull else BsonDouble(x.doubleValue)
  implicit def doubleToBsonDouble(x: Double) = BsonDouble(x)
  implicit def doubleToOptBsonDouble(x: java.lang.Double) =
    if (x == null) BsonNull else BsonDouble(x.doubleValue)
  implicit def dateToOptBsonDate(x: Date) =
    if (x == null) BsonNull else BsonDate(x)
  implicit def stringToOptBsonStr(x: String) =
    if (x == null) BsonNull else BsonStr(x)
  implicit def nullToBsonNull(x: Null) = BsonNull
}
