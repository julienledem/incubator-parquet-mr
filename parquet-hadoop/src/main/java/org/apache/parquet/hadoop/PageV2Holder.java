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
 * Page (v2) holder that holds on to page data and encoding etc.
 */
class PageV2Holder extends PageHolder {
  private final int rowCount;
  private final int nullCount;
  private final ByteBuffer repetitionLevels;
  private final ByteBuffer definitionLevels;
  private ByteBuffer values; // Values. position = 0 and limit = size
  private long uncompressedValuesSize;

  PageV2Holder(ByteBufferAllocator allocator, BytesCompressor compressor, int pageIndex, ColumnDescriptor path, int rowCount, int nullCount, int valueCount,
                      BytesInput repetitionLevels, BytesInput definitionLevels, Encoding dataEncoding, BytesInput values,
                      Statistics<?> statistics, boolean compressed, long uncompressedValuesSize)
    throws IOException{
    super(allocator, compressor, pageIndex, PageType.V2, path, valueCount, dataEncoding, statistics, compressed);
    this.rowCount = rowCount;
    this.nullCount = nullCount;
    this.repetitionLevels = copy(repetitionLevels);
    this.definitionLevels = copy(definitionLevels);
    this.values = copy(values);
    this.uncompressedValuesSize = uncompressedValuesSize;
  }

  int getRowCount() {
    return rowCount;
  }

  int getNullCount() {
    return nullCount;
  }

  ByteBuffer getRepetitionLevels() {
    return repetitionLevels;
  }

  ByteBuffer getDefinitionLevels() {
    return definitionLevels;
  }

  ByteBuffer getValues() {
    return values;
  }

  long getUncompressedValuesSize() {
    return uncompressedValuesSize;
  }

  public ByteBuffer getData() {
    return values;
  }

  @Override
  int getDataOffset() throws IOException {
    return 0;
  }

  @Override
  void updateData(BytesInput valuesBytes, Encoding encoding) throws IOException {
    this.uncompressedValuesSize = valuesBytes.size();
    this.values = compressFirstToReplaceSecond(valuesBytes, this.values);
    this.compressed = true;
    this.valuesEncoding = encoding;
  }

  @Override
  void compressIfNeeded() throws IOException {
    if (isCompressed()) {
      return;
    }
    this.values = compressFirstToReplaceSecond(BytesInput.from(values), this.values);
    this.compressed = true;
    this.valuesEncoding = getValuesEncoding();
  }

  @Override
  public ByteBuffer getValuesBytes() throws IOException {
    return values.slice();
  }

  @Override
  public void release() {
    release(repetitionLevels);
    release(definitionLevels);
    release(values);
  }
}
