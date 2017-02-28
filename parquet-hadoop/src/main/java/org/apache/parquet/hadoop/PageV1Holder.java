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

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;

/**
 * Page (v1) holder that holds on to page data and encoding etc.
 */
class PageV1Holder extends PageHolder {

  private final Encoding rlEncoding;
  private final Encoding dlEncoding;
  private ByteBuffer pageBody; // includes RLs, DLs, Values. position = 0 and limit = size
  private long uncompressedPageBodySize;

  PageV1Holder(ByteBufferAllocator allocator, BytesCompressor compressor, int pageIndex, ColumnDescriptor path, BytesInput bytes, int valueCount, Statistics<?> statistics,
                      Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding, boolean compressed, long uncompressedPageBodySize) throws IOException {
    super(allocator, compressor, pageIndex, PageType.V1, path, valueCount, valuesEncoding, statistics, compressed);
    this.pageBody = copy(bytes);
    this.uncompressedPageBodySize = uncompressedPageBodySize;
    this.rlEncoding = rlEncoding;
    this.dlEncoding = dlEncoding;
  }

  Encoding getRlEncoding() {
    return rlEncoding;
  }

  Encoding getDlEncoding() {
    return dlEncoding;
  }

  long getUncompressedPageBodySize() {
    return uncompressedPageBodySize;
  }

  ByteBuffer getPageBody() {
    return pageBody;
  }

  @Override
  int getDataOffset() throws IOException {
    final ValuesReader rlReader = rlEncoding.getValuesReader(getPath(), REPETITION_LEVEL);
    final ValuesReader dlReader = dlEncoding.getValuesReader(getPath(), DEFINITION_LEVEL);
    rlReader.initFromPage(getValueCount(), pageBody, 0);
    int next = rlReader.getNextOffset();
    dlReader.initFromPage(getValueCount(), pageBody, next);
    next = dlReader.getNextOffset();
    return next;
  }

  @Override
  void updateData(BytesInput valuesBytes, Encoding encoding) throws IOException {
    final BytesInput rldlBytes = BytesInput.from(pageBody, 0, getDataOffset());
    BytesInput uncompressedData = BytesInput.concat(rldlBytes, valuesBytes);
    this.uncompressedPageBodySize = uncompressedData.size();
    this.pageBody = compressFirstToReplaceSecond(uncompressedData, this.pageBody);
    this.compressed = true;
    this.valuesEncoding = encoding;
  }

  @Override
  void compressIfNeeded() throws IOException {
    if (isCompressed()) {
      return;
    }
    this.pageBody = compressFirstToReplaceSecond(BytesInput.from(this.pageBody, 0, (int)uncompressedPageBodySize), this.pageBody);
    this.compressed = true;
    this.valuesEncoding = getValuesEncoding();
  }

  @Override
  public ByteBuffer getValuesBytes() throws IOException {
    int dataOffset = getDataOffset();
    ByteBuffer b = pageBody.slice();
    b.position(dataOffset);
    return b;
  }

  @Override
  public void release() {
    release(pageBody);
  }

}
