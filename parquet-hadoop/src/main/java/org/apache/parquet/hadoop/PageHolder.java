/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;

/**
 * Base class for page holders.
 */
abstract class PageHolder {
  private final BytesCompressor compressor;
  private final ByteBufferAllocator allocator;
  private final int pageIndex;
  private final int valueCount;
  private final ColumnDescriptor path;
  private final Statistics<?> statistics;
  private final PageType type;
  protected boolean compressed;
  protected Encoding valuesEncoding;

  enum PageType {
    V1,
    V2
  }

  PageHolder(ByteBufferAllocator allocator, BytesCompressor compressor, int pageIndex, PageType type, ColumnDescriptor path, int valueCount,
                    Encoding valuesEncoding, Statistics<?> statistics, boolean compressed) throws IOException {
    this.allocator = allocator;
    this.compressor = compressor;
    this.pageIndex = pageIndex;
    this.path = path;
    this.statistics = statistics;
    this.valueCount = valueCount;
    this.valuesEncoding = valuesEncoding;
    this.type = type;
    this.compressed = compressed;
  }

  protected void release(ByteBuffer buffer) {
    allocator.release(buffer);
  }

  /**
   * Copy data on to byte buffer created using allocator
   * @param data data to copy
   * @return the copied data in a newly allocated buffer (position is 0 and limit is the size)
   * @throws IOException
   */
  protected ByteBuffer copy(BytesInput data) throws IOException {
    long inputSize = data.size();
    final ByteBuffer byteBuffer = allocator.allocate((int)data.size());
    byteBuffer.put(data.toByteArray());
    byteBuffer.flip();
    if (inputSize != byteBuffer.remaining()) {
      throw new IllegalStateException("input size should be the same as output size: " + inputSize + " != " + byteBuffer.remaining());
    }
    return byteBuffer;
  }

  protected ByteBuffer compressFirstToReplaceSecond(BytesInput uncompressedBytes, ByteBuffer toRelease) throws IOException {
    try {
      return copy(compressor.compress(uncompressedBytes));
    } finally {
      release(toRelease);
    }
  }

  int getPageIndex() {
    return pageIndex;
  }

  int getValueCount() {
    return valueCount;
  }

  int getNonNullValueCount() {
    return valueCount - (int)statistics.getNumNulls();
  }

  Statistics<?> getStatistics() {
    return statistics;
  }

  Encoding getValuesEncoding() {
    return valuesEncoding;
  }

  boolean isCompressed() {
    return compressed;
  }

  ColumnDescriptor getPath() {
    return path;
  }

  PageType getType() {
    return type;
  }

  abstract int getDataOffset() throws IOException;

  /**
   *
   * @param allocator
   * @param compressor
   * @param valuesBytes position and limit should be ready to read
   * @param encoding
   * @throws IOException
   */
  abstract void updateData(BytesInput valuesBytes, Encoding encoding) throws IOException;

  abstract void compressIfNeeded() throws IOException;

  abstract public ByteBuffer getValuesBytes() throws IOException;

  abstract public void release();

}
