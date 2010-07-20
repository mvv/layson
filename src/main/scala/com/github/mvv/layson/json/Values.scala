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

package com.github.mvv.layson.json

import com.github.mvv.layson.bson._

sealed trait JsonValue extends NotNull {
  def serialize: Iterator[Char]
  def toBson: BsonValue
}
sealed trait OptJsonValue extends JsonValue
sealed trait MandatoryJsonValue extends JsonValue

sealed trait OptJsonBool extends OptJsonValue {
  def toBson: OptBsonBool
}
object OptJsonBool {
  implicit def optJsonBoolToBoolean(x: OptJsonBool) = x match {
    case JsonNull => null
    case JsonBool(x) => java.lang.Boolean.valueOf(x)
  }
}
sealed trait JsonBool extends OptJsonBool with MandatoryJsonValue {
  def value: Boolean
  def toBson: BsonBool
}
object JsonBool {
  object True extends JsonBool {
    val value = true
    def serialize = "true".toIterator
    def toBson = BsonBool.True
  }
  object False extends JsonBool {
    val value = false
    def serialize = "false".toIterator
    def toBson = BsonBool.False
  }

  def apply(value: Boolean) = if (value) True else False
  def unapply(x: JsonBool): Option[Boolean] = Some(x.value)

  implicit def jsonBoolToBoolean(x: JsonBool) = x.value
}

sealed trait OptJsonNum extends OptJsonValue {
  def toBson: OptNumericBsonValue
}
final class JsonNum(val value: BigDecimal) extends OptJsonNum
                                              with MandatoryJsonValue {
  def serialize = value.toString.toIterator
  final def toBson: NumericBsonValue =
    if (value.scale == 0) {
      val l = value.longValue
      if (l < Int.MinValue || l > Int.MaxValue)
        BsonLong(l)
      else
        BsonInt(l.intValue)
    } else
      BsonDouble(value.doubleValue)
}
object JsonNum {
  def apply(value: Int) = new JsonNum(BigDecimal(value))
  def apply(value: Long) = new JsonNum(BigDecimal(value))
  def apply(value: Double) = new JsonNum(BigDecimal(value.toString))
  def apply(value: BigInt) = new JsonNum(BigDecimal(value))
  def apply(value: BigDecimal) = new JsonNum(value)
  def unapply(x: JsonNum): Option[BigDecimal] = Some(x.value)
}

sealed trait OptJsonStr extends OptJsonValue {
  def toBson: OptBsonStr
}
object OptJsonStr {
  implicit def optJsonStrToString(x: OptJsonStr) = x match {
    case JsonNull => null
    case JsonStr(x) => x
  }
}
trait JsonStr extends OptJsonStr with MandatoryJsonValue {
  def iterator: Iterator[Char]
  def value: String
  final def serialize = {
    val it = iterator
    Iterator.single('"') ++
    (it.map {
       case c if c == '"' || c == '\\' || c <= 0x1F =>
         ("\\u%04X" format c.intValue).toIterator
       case c => Iterator.single(c)
     } flatten) ++ Iterator.single('"')
  }
  def toBson: BsonStr = new JsonBsonStr(this)
}
class StrictJsonStr(val value: String) extends JsonStr {
  require(value != null)
  def iterator = value.iterator
}
class LazyJsonStr(it: Iterator[Char]) extends JsonStr {
  private lazy val chars = it.toStream
  def iterator = chars.iterator
  def value = chars.mkString
}
object JsonStr {
  def apply(value: String): JsonStr = new StrictJsonStr(value)
  def apply(it: Iterator[Char]): JsonStr = new LazyJsonStr(it)
  def unapply(x: JsonStr): Option[String] = Some(x.value)

  implicit def jsonStrToString(x: JsonStr) = x.value
}

trait OptCompoundJsonValue extends OptJsonValue
trait CompoundJsonValue extends OptCompoundJsonValue with MandatoryJsonValue

sealed trait OptJsonArray extends OptCompoundJsonValue {
  def toBson: OptBsonArray
}
trait JsonArray extends OptJsonArray with CompoundJsonValue {
  def iterator: Iterator[JsonValue]
  def elements: Seq[JsonValue]
  final def serialize = {
    val it = iterator
    Iterator.single('[') ++
    (it.zipWithIndex.map { case (element, i) =>
       (if (i == 0) Iterator.empty else Iterator.single(',')) ++
       element.serialize
     } flatten) ++ Iterator.single(']')
  }
  def toBson: BsonArray = new JsonBsonArray(this)
}
class StrictJsonArray(val elements: Seq[JsonValue]) extends JsonArray {
  def iterator = elements.iterator
}
class LazyJsonArray(it: Iterator[JsonValue]) extends JsonArray {
  lazy val elements = it.toStream
  def iterator = elements.iterator
}
object JsonArray {
  def apply(elem: JsonValue*) = new StrictJsonArray(elem)
  def apply(it: Iterator[JsonValue]) = new LazyJsonArray(it)
  def unapply(x: JsonArray): Option[Seq[JsonValue]] = Some(x.elements)
}

sealed trait OptJsonObject extends OptCompoundJsonValue {
  def toBson: OptBsonObject
}
trait JsonObject extends OptJsonObject with CompoundJsonValue {
  def iterator: Iterator[(String, JsonValue)]
  def members: Seq[(String, JsonValue)]
  def membersMap: Map[String, JsonValue]
  def get(key: String): Option[JsonValue]
  final def serialize = {
    val it = iterator
    Iterator.single('{') ++
    (it.zipWithIndex.map { case ((name, value), i) =>
       (if (i == 0) Iterator.empty else Iterator.single(',')) ++
       JsonStr(name).serialize ++ Iterator.single(':') ++ value.serialize
     } flatten) ++ Iterator.single('}')
  }
  def toBson: BsonObject = new JsonBsonObject(this)
}
class SeqJsonObject(val members: Seq[(String, JsonValue)]) extends JsonObject {
  def iterator = members.iterator
  def membersMap = members.toMap
  def get(key: String) = members.find(_._1 == key).map(_._2)
}
class MapJsonObject(val membersMap: Map[String, JsonValue]) extends JsonObject {
  def iterator = membersMap.iterator
  def members = membersMap.toSeq
  def get(key: String) = membersMap.get(key)
}
class LazyJsonObject(it: Iterator[(String, JsonValue)]) extends JsonObject {
  private lazy val mems = it.toStream
  def iterator = mems.iterator
  def members = mems
  def membersMap = mems.toMap
  override def get(key: String) = mems.find(_._1 == key).map(_._2)
}
object JsonObject {
  def apply() = new SeqJsonObject(Seq.empty)
  def apply[T <% JsonValue](
        member: (String, T), members: (String, JsonValue)*) =
    new SeqJsonObject(
          (member._1 -> implicitly[T => JsonValue].apply(member._2)) +: members)
  def apply(members: Seq[(String, JsonValue)]) = new SeqJsonObject(members)
  def apply(map: Map[String, JsonValue]) = new MapJsonObject(map)
  def apply(it: Iterator[(String, JsonValue)]) = new LazyJsonObject(it)
  def unapply(x: JsonObject): Option[Map[String, JsonValue]] =
    Option(x.membersMap)
}

object JsonNull extends OptJsonValue
                   with OptJsonBool
                   with OptJsonNum
                   with OptJsonStr
                   with OptJsonArray
                   with OptJsonObject {
  def serialize = "null".toIterator
  def toBson: BsonNull.type = BsonNull
}

object JsonValue {
  implicit def booleanToJsonBool(x: Boolean) = JsonBool(x)
  implicit def booleanToOptJsonBool(x: java.lang.Boolean) =
    if (x == null) JsonNull else JsonBool(x.booleanValue)
  implicit def stringToOptJsonStr(x: String) =
    if (x == null) JsonNull else JsonStr(x)
  implicit def nullToJsonNull(x: Null) = JsonNull
}

final class JsonBsonStr(underlying: JsonStr) extends BsonStr {
  def iterator = underlying.iterator
  def value = underlying.value
}
final class JsonBsonArray(underlying: JsonArray) extends BsonArray {
  def iterator = underlying.iterator.map(_.toBson)
  def elements = underlying.elements.view.map(_.toBson)
}
final class JsonBsonObject(underlying: JsonObject) extends BsonObject {
  def iterator = underlying.iterator.map { case (k, v) => k -> v.toBson }
  def members = underlying.members.view.map { case (k, v) => k -> v.toBson }
  def membersMap = underlying.membersMap.mapValues(_.toBson)
  def get(key: String) = underlying.get(key).map(_.toBson)
}
