/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class HopsworksRMAppCertificateActions implements RMAppCertificateActions {
  private static final Log LOG = LogFactory.getLog(HopsworksRMAppCertificateActions.class);
  
  private final Configuration conf;
  private final URL hopsworksHost;
  private final URL loginEndpoint;
  private final URL signEndpoint;
  private final String revokeEndpoint;
  
  public HopsworksRMAppCertificateActions(Configuration conf) throws MalformedURLException {
    this.conf = conf;
    
    // TODO(Antonis) Read them from configuration
    this.hopsworksHost = new URL("http://bbc4.sics.se:34821");
    this.loginEndpoint = new URL(hopsworksHost, "hopsworks-api/api/auth/login");
    this.signEndpoint = new URL(this.hopsworksHost, "sign/endpoint");
    this.revokeEndpoint = "";
  }
  
  @Override
  public byte[] sign(PKCS10CertificationRequest csr) throws URISyntaxException, IOException {
    BasicCookieStore cookieStore = new BasicCookieStore();
    CloseableHttpClient httpClient = HttpClients.custom()
        .setDefaultCookieStore(cookieStore)
        .build();
    HttpPost req = new HttpPost("http://bbc4.sics.se:34821/hopsworks-api/api/auth/login");
  
    HttpUriRequest login = RequestBuilder.post()
        .setUri(loginEndpoint.toURI())
        .addParameter("email", "admin@kth.se")
        .addParameter("password", "admin")
        .build();
    CloseableHttpResponse resp = httpClient.execute(login);
    LOG.info(resp);
  
    HttpEntity entity = resp.getEntity();
    LOG.info(entity);
  
    HttpGet getReq = new HttpGet("http://bbc4.sics.se:34821/hopsworks-api/api/admin/materializer/");
    CloseableHttpResponse resp2 = httpClient.execute(getReq);
    resp2.getStatusLine();
    HttpEntity entity1 = resp2.getEntity();
    String bla = EntityUtils.toString(entity1);
    LOG.info(bla);
    return new byte[0];
  }
}
