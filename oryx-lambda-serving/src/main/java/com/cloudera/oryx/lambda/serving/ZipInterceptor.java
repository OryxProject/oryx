package com.cloudera.oryx.lambda.serving;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

@Provider
public class ZipInterceptor implements ReaderInterceptor {

  /**
   * Interceptor method wrapping calls to {@link javax.ws.rs.ext.MessageBodyReader#readFrom} method.
   * <p/>
   * The parameters of the wrapped method called are available from {@code context}.
   * Implementations of this method SHOULD explicitly call {@link javax.ws.rs.ext.ReaderInterceptorContext#proceed}
   * to invoke the next interceptor in the chain, and ultimately the wrapped
   * {@link javax.ws.rs.ext.MessageBodyReader#readFrom} method.
   *
   * @param context invocation context.
   * @return result of next interceptor invoked or the wrapped method if last interceptor in chain.
   * @throws java.io.IOException                 if an IO error arises or is thrown by the wrapped
   *                                             {@code MessageBodyReader.readFrom} method.
   * @throws javax.ws.rs.WebApplicationException thrown by the wrapped {@code MessageBodyReader.readFrom} method.
   */
  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException, WebApplicationException {
    InputStream in = context.getInputStream();
    context.setInputStream(new ZipInputStream(in));
    try {
      return context.proceed();
    } finally {
      context.setInputStream(in);
    }
  }
}
