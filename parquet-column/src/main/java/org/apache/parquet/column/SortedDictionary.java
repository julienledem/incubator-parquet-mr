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
package org.apache.parquet.column;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.io.ParquetDecodingException;

import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

/**
 * Sorted dictionary for a given dictionary page.
 */
public class SortedDictionary {

  private final Dictionary dictionary;
  private final Map<Integer, Integer> sortedDictionaryId;
  private final DictionaryPage sortedDictionaryPage;

  public SortedDictionary(DictionaryPage dictionaryPage, final ColumnDescriptor columnDescriptor, ParquetProperties parquetProperties) throws IOException {
    this.sortedDictionaryId = new HashMap<Integer, Integer>();
    this.dictionary = dictionaryPage.getEncoding().initDictionary(columnDescriptor, dictionaryPage);

    final int[] indices = new int[dictionary.getMaxId() + 1];
    for (int i = 0; i < indices.length; ++i) {
      indices[i] = i;
    }

    IntArrays.quickSort(indices, getValueComparator(columnDescriptor));
    for (int i = 0; i < indices.length; ++i) {
      // map old dictionary id to new one
      sortedDictionaryId.put(indices[i], i);
    }

    // Create sorted dictionary page.
    final DictionaryValuesWriter dictionaryValuesWriter = parquetProperties.newDictionaryWriter(columnDescriptor);
    try {
      switch (columnDescriptor.getType()) {
        case BOOLEAN: {
          for (int i = 0; i < indices.length; ++i) {
            dictionaryValuesWriter.writeBoolean(dictionary.decodeToBoolean(indices[i]));
          }
          break;
        }

        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
        case INT96: {
          for (int i = 0; i < indices.length; ++i) {
            dictionaryValuesWriter.writeBytes(dictionary.decodeToBinary(indices[i]));
          }
          break;
        }

        case INT32: {
          for (int i = 0; i < indices.length; ++i) {
            dictionaryValuesWriter.writeInteger(dictionary.decodeToInt(indices[i]));
          }
          break;
        }

        case INT64: {
          for (int i = 0; i < indices.length; ++i) {
            dictionaryValuesWriter.writeLong(dictionary.decodeToLong(indices[i]));
          }
          break;
        }

        case DOUBLE: {
          for (int i = 0; i < indices.length; ++i) {
            dictionaryValuesWriter.writeDouble(dictionary.decodeToDouble(indices[i]));
          }
          break;
        }

        case FLOAT: {
          for (int i = 0; i < indices.length; ++i) {
            dictionaryValuesWriter.writeFloat(dictionary.decodeToFloat(indices[i]));
          }
          break;
        }

        default:
          throw new ParquetDecodingException("Dictionary encoding not supported for type: " + columnDescriptor.getType());
      }
      // Record dictionary size before calling toDictPageAndClose.
      // toDictPageAndClose() will not return dictionary page unless dictionary is used by value writer for value encoding.
      dictionaryValuesWriter.recordDictionarySize();
      this.sortedDictionaryPage = dictionaryValuesWriter.toDictPageAndClose();
    } finally {
      dictionaryValuesWriter.close();
    }
  }

  private IntComparator getValueComparator(ColumnDescriptor columnDescriptor) {
    switch (columnDescriptor.getType()) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
        return new AbstractIntComparator() {
          @Override
          public int compare(int o1, int o2) {
            // TODO address concerns in PARQUET-686
            return dictionary.decodeToBinary(o1).compareTo(dictionary.decodeToBinary(o2));
          }
        };
      case INT32:
        return new AbstractIntComparator() {
          @Override
          public int compare(int o1, int o2) {
            return Integer.compare(dictionary.decodeToInt(o1), dictionary.decodeToInt(o2));
          }
        };
      case INT64:
        return new AbstractIntComparator() {
          @Override
          public int compare(int o1, int o2) {
            return Long.compare(dictionary.decodeToLong(o1), dictionary.decodeToLong(o2));
          }
        };
      case DOUBLE:
        return new AbstractIntComparator() {
          @Override
          public int compare(int o1, int o2) {
            return Double.compare(dictionary.decodeToDouble(o1), dictionary.decodeToDouble(o2));
          }
        };
      case FLOAT:
        return new AbstractIntComparator() {
          @Override
          public int compare(int o1, int o2) {
            return Float.compare(dictionary.decodeToFloat(o1), dictionary.decodeToFloat(o2));
          }
        };
      default:
        throw new ParquetDecodingException("Dictionary encoding not supported for type: " + columnDescriptor.getType());
    }
  }

  public Dictionary getDictionary() {
    return dictionary;
  }

  public int getNewId(int id) {
    return sortedDictionaryId.get(id);
  }

  public DictionaryPage getSortedDictionaryPage() {
    return sortedDictionaryPage;
  }
}
