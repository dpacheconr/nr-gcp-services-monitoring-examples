package app;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

// // New Relic API imports
import com.newrelic.api.agent.*;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.gson.GsonFactory;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.protocol.HTTP;

public class HelloWorld implements HttpFunction {
  private static final Logger logger = Logger.getLogger(HelloWorld.class.getName());

  //Manual instrumentation is required to propagate distributed trace headers between two services when propagation cannot be achieved automatically via New Relic agent adding HTTP headers to a service's outbound requests.
  //Example to generate DT headers
  public String addDistributedTraceHeadersToPass() {
      // ConcurrentHashMapHeaders provides a concrete implementation of com.newrelic.api.agent.Headers
      Headers distributedTraceHeaders = ConcurrentHashMapHeaders.build(HeaderType.MESSAGE);
      // Generate W3C Trace Context headers and insert them into the distributedTraceHeaders map
      NewRelic.getAgent().getTransaction().insertDistributedTraceHeaders(distributedTraceHeaders);
      String traceparent = distributedTraceHeaders.getHeader("traceparent");
      System.out.println("DT headers to send "+traceparent);
      return traceparent;
  }
  //Example to accept DT headers
  public void DistributedTraceHeadersToaccept(HttpRequest req) {
      // ConcurrentHashMapHeaders provides a concrete implementation of com.newrelic.api.agent.Headers
      Headers distributedTraceHeaders = ConcurrentHashMapHeaders.build(HeaderType.MESSAGE);
      // Generate W3C Trace Context headers and insert them into the distributedTraceHeaders map
      String traceparent = req.getFirstQueryParameter("ptrace").get();
      // ptrace is a custom header created by the demo producer service, where to obtain the headers from will depend on transport used between the services, i.e. messaging queues.
      distributedTraceHeaders.addHeader("traceparent", traceparent);
      System.out.println("DT headers received "+traceparent);
      NewRelic.getAgent().getTransaction().acceptDistributedTraceHeaders(TransportType.HTTPS, distributedTraceHeaders);
    }

  @Override
  public void service(HttpRequest request, HttpResponse response)
      throws IOException {
    BufferedWriter writer = response.getWriter();
    writer.write("Well done on instrumenting your google cloud functions");
    DistributedTraceHeadersToaccept(request);
    String traceparent=addDistributedTraceHeadersToPass();
    
    String webServerUrl = "https:/YOUR_COMPOSER_URI-dot-us-central1.composer.googleusercontent.com";
    String dagName = "YOUR_DAG_NAME";
    String url = String.format("%s/api/v1/dags/%s/dagRuns", webServerUrl, dagName);

    GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault()
        .createScoped("https://www.googleapis.com/auth/cloud-platform");
    HttpCredentialsAdapter credentialsAdapter = new HttpCredentialsAdapter(googleCredentials);
    HttpRequestFactory requestFactory =
      new NetHttpTransport().createRequestFactory(credentialsAdapter);

    Map<String, Map<String, String>> json = new HashMap<String, Map<String, String>>();
    Map<String, String> conf = new HashMap<String, String>();
    conf.put("traceparent", traceparent);
    json.put("conf", conf);
    com.google.api.client.http.HttpContent content = new JsonHttpContent(new GsonFactory(), json);
    com.google.api.client.http.HttpRequest request_out = requestFactory.buildPostRequest(new GenericUrl(url), content);
    request_out.getHeaders().setContentType("application/json");
    com.google.api.client.http.HttpResponse response_out;
    try {
      response_out = request_out.execute();
      int statusCode = response_out.getStatusCode();
      logger.info("Response code: " + statusCode);
      logger.info(response_out.parseAsString());
    } catch (HttpResponseException e) {
      // https://cloud.google.com/java/docs/reference/google-http-client/latest/com.google.api.client.http.HttpResponseException
      logger.info("Received HTTP exception");
      logger.info(e.getLocalizedMessage());
      logger.info("- 400 error: wrong arguments passed to Airflow API");
      logger.info("- 401 error: check if service account has Composer User role");
      logger.info("- 403 error: check Airflow RBAC roles assigned to service account");
      logger.info("- 404 error: check Web Server URL");
    } catch (Exception e) {
      logger.info("Received exception");
      logger.info(e.getLocalizedMessage());
    }
  }
   
}


