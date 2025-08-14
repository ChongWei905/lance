/*
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
package com.lancedb.lance;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

public class ArrowAddressStreamReader extends ArrowReader {

  private final ArrowAddressResult result;
  private int batchRead = 0;
  private VectorSchemaRoot curBatch;
  private final long totalLength;

  public ArrowAddressStreamReader(BufferAllocator allocator, ArrowAddressResult result) {
    super(allocator);
    this.result = result;
    this.totalLength = result.getArrayAddresses().length;
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    return curBatch;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (batchRead >= totalLength) {
      return false;
    }
    long arrayAddr = result.getArrayAddresses()[batchRead];
    long schemaAddr = result.getSchemaAddresses()[batchRead];
    try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr);
        ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr)) {
      if (curBatch != null) {
        curBatch.close();
      }
      curBatch = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null);
    }
    batchRead++;
    return true;
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    curBatch.close();
  }

  @Override
  protected Schema readSchema() throws IOException {
    if (result != null && totalLength > 0) {
      long schemaAddr = result.getSchemaAddresses()[0];
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr)) {
        return Data.importSchema(allocator, arrowSchema, null);
      }
    } else {
      return null;
    }
  }
}
