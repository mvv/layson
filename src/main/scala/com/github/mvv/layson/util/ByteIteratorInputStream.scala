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

import java.io.{InputStream, IOException}

class ByteIteratorInputStream private() extends InputStream {
  private var it: Iterator[Byte] = null
  private var closed = false

  def this(it: Iterator[Byte]) = {
    this()
    this.it = it
  }

  override def close() {
    closed = true
    it = null
  }

  def read(): Int = {
    if (closed)
      throw new IOException
    if (it == null)
      -1
    else if (it.hasNext)
      it.next
    else {
      it = null
      -1
    }
  }

  override def read(buf: Array[Byte]): Int = read(buf, 0, buf.size)

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (buf == null)
      throw new NullPointerException
    if (off < 0 || len < 0 || off + len > buf.length)
      throw new IndexOutOfBoundsException
    if (closed)
      throw new IOException

    if (len == 0)
      0
    else {
      var i = off
      it.take(len).foreach { c =>
        buf(off + i) = c
        i += 1
      }
      i -= off
      if (i == 0) {
        it = null
        -1
      } else
        i
    }
  }
}
