/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.traffic;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.traffic.als.ALSEndpoint;
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * Simple utility class for sending traffic to an Oryx cluster for an extended period of time.
 * Required args:
 *
 * <ol>
 *   <li>{@code hosts} comma-separated distinct host:port pairs to send HTTP requests to</li>
 *   <li>{@code requestIntervalMS} average delay between requests in MS</li>
 *   <li>{@code threads} number of concurrent requests</li>
 * </ol>
 *
 * These can be followed by more args that are passed to subclasses of {@link Endpoint}.
 */
public final class TrafficUtil {

  private static final Logger log = LoggerFactory.getLogger(TrafficUtil.class);
  private static final Pattern COMMA = Pattern.compile(",");

  private TrafficUtil() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("usage: TrafficUtil [hosts] [requestIntervalMS] [threads] [... other args]");
      return;
    }

    String[] hostStrings = COMMA.split(args[0]);
    Preconditions.checkArgument(hostStrings.length >= 1);
    int requestIntervalMS = Integer.parseInt(args[1]);
    Preconditions.checkArgument(requestIntervalMS >= 0);
    final int numThreads = Integer.parseInt(args[2]);
    Preconditions.checkArgument(numThreads >= 1);

    final String[] otherArgs = new String[args.length - 3];
    System.arraycopy(args, 3, otherArgs, 0, otherArgs.length);

    final List<URI> hosts = new ArrayList<>(hostStrings.length);
    for (String hostString : hostStrings) {
      hosts.add(URI.create(hostString));
    }

    final int perClientRequestIntervalMS = numThreads * requestIntervalMS;

    final Endpoints alsEndpoints = new Endpoints(ALSEndpoint.buildALSEndpoints());
    final AtomicLong requestCount = new AtomicLong();
    final AtomicLong serverErrorCount = new AtomicLong();
    final AtomicLong clientErrorCount = new AtomicLong();
    final AtomicLong exceptionCount = new AtomicLong();

    final long start = System.currentTimeMillis();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      for (int i = 0; i < numThreads; i++) {
        executor.submit(new LoggingRunnable() {
          @Override
          public void doRun() {

            RandomGenerator random = RandomManager.getRandom();
            ExponentialDistribution msBetweenRequests;
            if (perClientRequestIntervalMS > 0) {
              msBetweenRequests = new ExponentialDistribution(random, perClientRequestIntervalMS);
            } else {
              msBetweenRequests = null;
            }

            ClientConfig clientConfig = new ClientConfig();
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
            connectionManager.setMaxTotal(numThreads);
            connectionManager.setDefaultMaxPerRoute(numThreads);
            clientConfig.property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager);
            clientConfig.connectorProvider(new ApacheConnectorProvider());
            Client client = ClientBuilder.newClient(clientConfig);

            try {
              while (true) {
                try {
                  WebTarget target = client.target("http://" + hosts.get(random.nextInt(hosts.size())));
                  Endpoint endpoint = alsEndpoints.chooseEndpoint(random);
                  Invocation invocation = endpoint.makeInvocation(target, otherArgs, random);

                  long startTime = System.currentTimeMillis();
                  Response response = invocation.invoke();
                  try {
                    response.readEntity(String.class);
                  } finally {
                    response.close();
                  }
                  long elapsedMS = System.currentTimeMillis() - startTime;

                  int statusCode = response.getStatusInfo().getStatusCode();
                  if (statusCode >= 400) {
                    if (statusCode >= 500) {
                      serverErrorCount.incrementAndGet();
                    } else {
                      clientErrorCount.incrementAndGet();
                    }
                    //log.warn("{}", response);
                  }

                  endpoint.recordTiming(elapsedMS);

                  if (requestCount.incrementAndGet() % 10000 == 0) {
                    long elapsed = System.currentTimeMillis() - start;
                    log.info("{}ms:\t{} requests\t({} client errors\t{} server errors\t{} exceptions)",
                             elapsed,
                             requestCount.get(),
                             clientErrorCount.get(),
                             serverErrorCount.get(),
                             exceptionCount.get());
                    for (Endpoint e : alsEndpoints.getEndpoints()) {
                      log.info("{}", e);
                    }
                  }

                  if (msBetweenRequests != null) {
                    int desiredElapsedMS = (int) Math.round(msBetweenRequests.sample());
                    if (elapsedMS < desiredElapsedMS) {
                      Thread.sleep(desiredElapsedMS - elapsedMS);
                    }
                  }
                } catch (Exception e) {
                  exceptionCount.incrementAndGet();
                  log.warn("{}", e.getMessage());
                }
              }
            } finally {
              client.close();
            }
          }
        });
      }
    } finally {
      executor.shutdown();
    }
  }

}
