/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink.doris;

import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class BatchBufferHttpEntity extends AbstractHttpEntity {
  protected static final int OUTPUT_BUFFER_SIZE = 4096;
  private final List<byte[]> buffer;
  private final long contentLength;

  public BatchBufferHttpEntity(List<byte[]> buffer, long bufferSizeBytes) {
    this.buffer = buffer;
    this.contentLength = bufferSizeBytes;
  }

  @Override
  public boolean isRepeatable() {
    return true;
  }

  @Override
  public boolean isChunked() {
    return false;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public InputStream getContent() {
    return new BatchBufferStream(buffer);
  }

  @Override
  public void writeTo(OutputStream outStream) throws IOException {
    try (InputStream inStream = new BatchBufferStream(buffer)) {
      final byte[] buffer = new byte[OUTPUT_BUFFER_SIZE];
      int readLen;
      while ((readLen = inStream.read(buffer)) != -1) {
        outStream.write(buffer, 0, readLen);
      }
    }
  }

  @Override
  public boolean isStreaming() {
    return false;
  }
}
