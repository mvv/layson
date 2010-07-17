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

package com.github.mvv.layson.util

import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.{ByteBuffer, CharBuffer}

object IteratorUtils {
  class CharIteratorOps(it: Iterator[Char]) {
    def toReader = IteratorUtils.toReader(it)
    def encode(charset: Charset,
               onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE,
               onUnmappableCharacter: CodingErrorAction = CodingErrorAction.REPLACE) =
      IteratorUtils.encode(it, charset, onMalformedInput, onUnmappableCharacter)
  }
  class ByteIteratorOps(it: Iterator[Byte]) {
    def toInputStream = IteratorUtils.toInputStream(it)
  }

  object implicits {
    implicit def laysonUtilsCharIteratorOps(it: Iterator[Char]) =
      new CharIteratorOps(it)
    implicit def laysonUtilsByteIteratorOps(it: Iterator[Byte]) =
      new ByteIteratorOps(it)
  }

  def toReader(it: Iterator[Char]) = new CharIteratorReader(it)
  def toInputStream(it: Iterator[Byte]) = new ByteIteratorInputStream(it)

  def encode(it: Iterator[Char], charset: Charset,
             onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE,
             onUnmappableCharacter: CodingErrorAction = CodingErrorAction.REPLACE) = {
    val encoder = charset.newEncoder
    encoder.onMalformedInput(onMalformedInput)
    encoder.onUnmappableCharacter(onUnmappableCharacter)
    val cb = CharBuffer.allocate(1)
    val bb = ByteBuffer.wrap(new Array[Byte](encoder.maxBytesPerChar.intValue))
    it.map { c =>
      cb.clear()
      cb.put(c)
      cb.flip()
      bb.clear()
      val cr = encoder.encode(cb, bb, true)
      bb.flip()
      if (!cr.isUnderflow)
        cr.throwException
      bb.array.toIterator.take(bb.limit)
    } .flatten ++ {
      bb.clear()
      val cr = encoder.flush(bb)
      bb.flip()
      if (!cr.isUnderflow)
        cr.throwException
      bb.array.toIterator.take(bb.limit)
    }
  }
}
