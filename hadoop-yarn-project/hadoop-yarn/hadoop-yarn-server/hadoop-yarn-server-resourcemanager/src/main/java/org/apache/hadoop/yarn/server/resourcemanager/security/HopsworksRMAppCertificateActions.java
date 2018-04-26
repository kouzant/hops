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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.util.io.pem.PemObjectGenerator;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;

public class HopsworksRMAppCertificateActions implements RMAppCertificateActions, Configurable {
  private static final Log LOG = LogFactory.getLog(HopsworksRMAppCertificateActions.class);
  private static final Set<Integer> ACCEPTABLE_HTTP_RESPONSES = new HashSet<>(2);
  
  private Configuration conf;
  private URL hopsworksHost;
  private  URL loginEndpoint;
  private  URL signEndpoint;
  private  URL revokeEndpoint;
  private  CertificateFactory certificateFactory;
  
  public HopsworksRMAppCertificateActions() throws MalformedURLException, GeneralSecurityException {
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_OK);
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_NO_CONTENT);
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void init() throws MalformedURLException, GeneralSecurityException {
    // TODO(Antonis) Read them from configuration
    hopsworksHost = new URL("http://bbc4.sics.se:34821");
    loginEndpoint = new URL(hopsworksHost, "hopsworks-api/api/auth/login");
    signEndpoint = new URL(this.hopsworksHost, "hopsworks-ca/ca/agentservice/sign/app");
    revokeEndpoint = new URL(this.hopsworksHost, "hopsworks-ca/ca/agentservice/revoke");
    certificateFactory = CertificateFactory.getInstance("X.509", "BC");
  }
  
  @Override
  public X509Certificate sign(PKCS10CertificationRequest csr) throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpClient httpClient = null;
    try {
      httpClient = createHttpClient();
      login(httpClient);
  
      String csrStr = stringifyCSR(csr);
      JsonObject json = new JsonObject();
      json.addProperty("csr", csrStr);
      
      CloseableHttpResponse signResponse = post(httpClient, json, signEndpoint.toURI(),
          "Hopsworks CA could not sign CSR");
      
      String signResponseEntity = EntityUtils.toString(signResponse.getEntity());
      JsonObject jsonResponse = new JsonParser().parse(signResponseEntity).getAsJsonObject();
      String signedCert = jsonResponse.get("pubAgentCert").getAsString();
      return parseCertificate(signedCert);
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
  
  @Override
  public int revoke(String certificateIdentifier) throws URISyntaxException, IOException {
    CloseableHttpClient httpClient = null;
    try {
      httpClient = createHttpClient();
      login(httpClient);
      
      JsonObject json = new JsonObject();
      json.addProperty("identifier", certificateIdentifier);
      
      CloseableHttpResponse response = post(httpClient, json, revokeEndpoint.toURI(),
          "Hopsworks CA could not revoke certificate " + certificateIdentifier);
      return response.getStatusLine().getStatusCode();
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
  
  private CloseableHttpClient createHttpClient() {
    BasicCookieStore cookieStore = new BasicCookieStore();
    return HttpClients.custom().setDefaultCookieStore(cookieStore).build();
  }
  
  private void login(CloseableHttpClient httpClient) throws URISyntaxException, IOException {
    HttpUriRequest login = RequestBuilder.post()
        .setUri(loginEndpoint.toURI())
        .addParameter("email", "agent@hops.io")
        .addParameter("password", "admin")
        .build();
    CloseableHttpResponse loginResponse = httpClient.execute(login);
    
    checkHTTPResponseCode(loginResponse.getStatusLine().getStatusCode(), "Could not login to Hopsworks");
  }
  
  private CloseableHttpResponse post(CloseableHttpClient httpClient, JsonObject jsonEntity, URI target, String errorMessage)
      throws IOException {
    HttpPost request = new HttpPost(target);
    request.setEntity(new StringEntity(jsonEntity.toString()));
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response.getStatusLine().getStatusCode(), errorMessage);
    return response;
  }
  
  private X509Certificate parseCertificate(String certificateStr) throws IOException, GeneralSecurityException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(certificateStr.getBytes())) {
      return (X509Certificate) certificateFactory.generateCertificate(bis);
    }
  }
  
  private void checkHTTPResponseCode(int responseCode, String msg) throws IOException {
    if (!ACCEPTABLE_HTTP_RESPONSES.contains(responseCode)) {
      throw new IOException("HTTP error, response code " + responseCode + " Message: " + msg);
    }
  }
  
  private String stringifyCSR(PKCS10CertificationRequest csr) throws IOException {
    try (StringWriter sw = new StringWriter()) {
      PemWriter pw = new PemWriter(sw);
      PemObjectGenerator pog = new JcaMiscPEMGenerator(csr);
      pw.writeObject(pog.generate());
      pw.flush();
      sw.flush();
      pw.close();
      return sw.toString();
    }
  }
}
