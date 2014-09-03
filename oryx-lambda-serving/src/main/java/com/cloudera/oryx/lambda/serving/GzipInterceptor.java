/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.lambda.serving;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

@Provider
public final class GzipInterceptor implements ReaderInterceptor {

  /**
   * <p>Interceptor method wrapping calls to {@link javax.ws.rs.ext.MessageBodyReader#readFrom} method.</p>
   *
   * <p>The parameters of the wrapped method called are available from {@code context}.
   * Implementations of this method SHOULD explicitly call {@link ReaderInterceptorContext#proceed}
   * to invoke the next interceptor in the chain, and ultimately the wrapped
   * {@link javax.ws.rs.ext.MessageBodyReader#readFrom} method.</p>
   *
   * @param context invocation context.
   * @return result of next interceptor invoked or the wrapped method if last interceptor in chain.
   * @throws IOException if an IO error arises or is thrown by the wrapped
   *  {@code MessageBodyReader.readFrom} method.
   * @throws javax.ws.rs.WebApplicationException thrown by the wrapped
   *  {@code MessageBodyReader.readFrom} method.
   */
  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException {
    InputStream in = context.getInputStream();
    context.setInputStream(new GZIPInputStream(in));
    try {
      return context.proceed();
    } finally {
      context.setInputStream(in);
    }
  }
}
