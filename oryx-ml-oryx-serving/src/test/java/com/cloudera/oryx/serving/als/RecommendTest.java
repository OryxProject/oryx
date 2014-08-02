package com.cloudera.oryx.serving.als;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.core.Application;

import static junit.framework.TestCase.assertNotNull;

public class RecommendTest extends JerseyTest {
 
    @Override
    protected Application configure() {
        return new ResourceConfig(Recommend.class);
    }
 
    @Test
    public void test() {
        final String recommend = target("recommend/foo").request().get(String.class);
        assertNotNull(recommend);
    }
}
