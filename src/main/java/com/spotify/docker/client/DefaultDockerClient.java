/*
 * Copyright (c) 2014 Spotify AB.
 *
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

package com.spotify.docker.client;

import com.google.common.io.CharStreams;
import com.google.common.net.HostAndPort;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.spotify.docker.client.messages.AuthConfig;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Image;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.ProgressMessage;
import com.spotify.docker.client.messages.RemovedImage;
import com.spotify.docker.client.messages.Version;

import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.ResponseProcessingException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.newHashMap;
import static com.spotify.docker.client.CompressedDirectory.delete;
import static com.spotify.docker.client.ObjectMapperProvider.objectMapper;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.HttpMethod.DELETE;
import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;

public class DefaultDockerClient implements DockerClient, Closeable {

  public static final String DEFAULT_UNIX_ENDPOINT = "unix:///var/run/docker.sock";
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 2375;

  private static final String UNIX_SCHEME = "unix";

  private static final String VERSION = "v1.12";
  private static final Logger log = LoggerFactory.getLogger(DefaultDockerClient.class);

  public static final long NO_TIMEOUT = 0;

  private static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = SECONDS.toMillis(5);
  private static final long DEFAULT_READ_TIMEOUT_MILLIS = SECONDS.toMillis(30);
  private static final int DEFAULT_CONNECTION_POOL_SIZE = 100;

  private static final Pattern CONTAINER_NAME_PATTERN = Pattern.compile("/?[a-zA-Z0-9_-]+");

  private static final GenericType<List<Container>> CONTAINER_LIST =
      new GenericType<List<Container>>() {};

  private static final GenericType<List<Image>> IMAGE_LIST =
      new GenericType<List<Image>>() {};

  private static final GenericType<List<RemovedImage>> REMOVED_IMAGE_LIST =
      new GenericType<List<RemovedImage>>() {};

  private final Client client;
  private final Client noTimeoutClient;

  private final URI uri;
  private final AuthConfig authConfig;
  
  private final PoolingClientConnectionManager cm;
  private final PoolingClientConnectionManager noTimeoutCm;

  Client getClient() {
    return client;
  }

  Client getNoTimeoutClient() {
    return noTimeoutClient;
  }

  PoolingClientConnectionManager getCm() {
    return cm;
  }

  PoolingClientConnectionManager getNoTimeoutCm() {
    return noTimeoutCm;
  }

  /**
   * Create a new client with default configuration.
   * @param uri The docker rest api uri.
   */
  public DefaultDockerClient(final String uri) {
    this(URI.create(uri.replaceAll("^unix:///", "unix://localhost/")));
  }

  /**
   * Create a new client with default configuration.
   * @param uri The docker rest api uri.
   */
  public DefaultDockerClient(final URI uri) {
    this(new Builder().uri(uri));
  }

  /**
   * Create a new client with default configuration.
   * @param uri The docker rest api uri.
   * @param dockerCertificates The certificates to use for HTTPS.
   */
  public DefaultDockerClient(final URI uri, final DockerCertificates dockerCertificates) {
    this(new Builder().uri(uri).dockerCertificates(dockerCertificates));
  }
  
  /**
   * Create a new client using the configuration of the builder.
   */
  protected DefaultDockerClient(final Builder builder) {
    URI originalUri = checkNotNull(builder.uri, "uri");

    if ((builder.dockerCertificates != null) && !originalUri.getScheme().equals("https")) {
      throw new IllegalArgumentException("https URI must be provided to use certificates");
    }

    if (originalUri.getScheme().equals(UNIX_SCHEME)) {
      this.uri = UnixConnectionSocketFactory.sanitizeUri(originalUri);
    } else {
      this.uri = originalUri;
    }

    cm = getConnectionManager(builder);
    noTimeoutCm = getConnectionManager(builder);

    BasicHttpParams params = new BasicHttpParams();
    HttpConnectionParams.setSoTimeout(params, (int) builder.readTimeoutMillis);
    HttpConnectionParams.setConnectionTimeout(params, (int) builder.connectTimeoutMillis);
    HttpClientParams.setConnectionManagerTimeout(params, (int) builder.connectTimeoutMillis);
    
    DefaultHttpClient httpClient = new DefaultHttpClient(cm, params);
    ApacheHttpClient4Engine engine = new ApacheHttpClient4Engine(httpClient, true);

    if (builder.dockerCertificates != null) {
      engine.setHostnameVerifier(builder.dockerCertificates.hostnameVerifier());
      engine.setSslContext(builder.dockerCertificates.sslContext());
    }

    this.client =
        new ResteasyClientBuilder().httpEngine(engine).register(ObjectMapperProvider.class)
            .register(LogsResponseReader.class).register(ProgressResponseReader.class).build();

    BasicHttpParams noTimeoutParams = new BasicHttpParams();
    HttpConnectionParams.setSoTimeout(noTimeoutParams, (int) builder.readTimeoutMillis);
    HttpConnectionParams.setConnectionTimeout(noTimeoutParams, (int) NO_TIMEOUT);
    HttpClientParams.setConnectionManagerTimeout(noTimeoutParams, (int) NO_TIMEOUT);

    DefaultHttpClient noTimeoutHttpClient = new DefaultHttpClient(noTimeoutCm, noTimeoutParams);
    ApacheHttpClient4Engine noTimeoutEngine =
        new ApacheHttpClient4Engine(noTimeoutHttpClient, true);

    if (builder.dockerCertificates != null) {
      noTimeoutEngine.setHostnameVerifier(builder.dockerCertificates.hostnameVerifier());
      noTimeoutEngine.setSslContext(builder.dockerCertificates.sslContext());
    }

    this.noTimeoutClient =
        new ResteasyClientBuilder().httpEngine(noTimeoutEngine)
            .register(ObjectMapperProvider.class).register(LogsResponseReader.class)
            .register(ProgressResponseReader.class).build();

    this.authConfig = builder.authConfig;
  }

  private PoolingClientConnectionManager getConnectionManager(Builder builder) {
    final PoolingClientConnectionManager cm =
        new PoolingClientConnectionManager(getSchemeRegistry(builder));

    // Use all available connections instead of artificially limiting ourselves to 2 per server.
    cm.setMaxTotal(builder.connectionPoolSize);
    cm.setDefaultMaxPerRoute(cm.getMaxTotal());

    return cm;
  }

  private SchemeRegistry getSchemeRegistry(final Builder builder) {
    final SchemeRegistry registry = new SchemeRegistry();
    final SSLSocketFactory https;
    if (builder.dockerCertificates == null) {
      https = SSLSocketFactory.getSocketFactory();
    } else {
      https = new SSLSocketFactory(builder.dockerCertificates.sslContext(),
                                   builder.dockerCertificates.hostnameVerifier());
    }

    registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
    registry.register(new Scheme("https", 443, https));

    if (builder.uri.getScheme().equals(UNIX_SCHEME)) {
      registry.register(new Scheme(UNIX_SCHEME, 80, new UnixConnectionSocketFactory(builder.uri)));
    }

    return registry;
  }

  @Override
  public void close() {
    client.close();
    noTimeoutClient.close();
  }

  @Override
  public String ping() throws DockerException, InterruptedException {
    final WebTarget resource = client.target(uri).path("_ping");
    return request(GET, String.class, resource, resource.request());
  }

  @Override
  public Version version() throws DockerException, InterruptedException {
    final WebTarget resource = resource().path("version");
    return request(GET, Version.class, resource, resource.request(APPLICATION_JSON_TYPE));
  }

  @Override
  public Info info() throws DockerException, InterruptedException {
    final WebTarget resource = resource().path("info");
    return request(GET, Info.class, resource, resource.request(APPLICATION_JSON_TYPE));
  }

  @Override
  public List<Container> listContainers(final ListContainersParam... params)
      throws DockerException, InterruptedException {
    WebTarget resource = resource()
        .path("containers").path("json");

    for (ListContainersParam param : params) {
      resource = resource.queryParam(param.name(), param.value());
    }

    return request(GET, CONTAINER_LIST, resource, resource.request(APPLICATION_JSON_TYPE));
  }

  @Override
  public List<Image> listImages(ListImagesParam... params)
      throws DockerException, InterruptedException {
    WebTarget resource = resource()
        .path("images").path("json");

    final Map<String, String> filters = newHashMap();
    for (ListImagesParam param : params) {
      if (param instanceof ListImagesFilterParam) {
        filters.put(param.name(), param.value());
      } else {
        resource = resource.queryParam(param.name(), param.value());
      }
    }

    // If filters were specified, we must put them in a JSON object and pass them using the
    // 'filters' query param like this: filters={"dangling":["true"]}
    try {
      if (!filters.isEmpty()) {
        final StringWriter writer = new StringWriter();
        final JsonGenerator generator = objectMapper().getFactory().createGenerator(writer);
        generator.writeStartObject();
        for (Map.Entry<String, String> entry : filters.entrySet()) {
          generator.writeArrayFieldStart(entry.getKey());
          generator.writeString(entry.getValue());
          generator.writeEndArray();
        }
        generator.writeEndObject();
        generator.close();
        // We must URL encode the string, otherwise Jersey chokes on the double-quotes in the json.
        final String encoded = URLEncoder.encode(writer.toString(), UTF_8.name());
        resource = resource.queryParam("filters", encoded);
      }
    } catch (IOException e) {
      throw new DockerException(e);
    }

    return request(GET, IMAGE_LIST, resource, resource.request(APPLICATION_JSON_TYPE));
  }

  @Override
  public ContainerCreation createContainer(final ContainerConfig config)
      throws DockerException, InterruptedException {
    return createContainer(config, null);
  }

  @Override
  public ContainerCreation createContainer(final ContainerConfig config,
                                           final String name)
      throws DockerException, InterruptedException {
    WebTarget resource = resource()
        .path("containers").path("create");

    if (name != null) {
      checkArgument(CONTAINER_NAME_PATTERN.matcher(name).matches(),
                    "Invalid container name: \"%s\"", name);
      resource = resource.queryParam("name", name);
    }

    log.info("Creating container with ContainerConfig: {}", config);

    try {
      return request(POST, ContainerCreation.class, resource, resource
          .request(APPLICATION_JSON_TYPE), Entity.json(config));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(config.image(), e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void startContainer(final String containerId)
      throws DockerException, InterruptedException {
    startContainer(containerId, HostConfig.builder().build());
  }

  @Override
  public void startContainer(final String containerId, final HostConfig hostConfig)
      throws DockerException, InterruptedException {
    checkNotNull(containerId, "containerId");
    checkNotNull(hostConfig, "hostConfig");

    log.info("Starting container with HostConfig: {}", hostConfig);

    try {
      final WebTarget resource = resource()
          .path("containers").path(containerId).path("start");
      request(POST, resource, resource
                  .request(APPLICATION_JSON_TYPE),
              Entity.json(hostConfig));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void pauseContainer(final String containerId)
      throws DockerException, InterruptedException {
    checkNotNull(containerId, "containerId");

    try {
      final WebTarget resource = resource()
          .path("containers").path(containerId).path("pause");
      request(POST, resource, resource.request());
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void unpauseContainer(final String containerId)
      throws DockerException, InterruptedException {
    checkNotNull(containerId, "containerId");

    try {
      final WebTarget resource = resource()
          .path("containers").path(containerId).path("unpause");
      request(POST, resource, resource.request());
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void restartContainer(String containerId) throws DockerException, InterruptedException {
    restartContainer(containerId, 10);
  }

  @Override
  public void restartContainer(String containerId, int secondsToWaitBeforeRestart)
      throws DockerException, InterruptedException {
    checkNotNull(containerId, "containerId");
    checkNotNull(secondsToWaitBeforeRestart, "secondsToWait");
    try {
      final WebTarget resource = resource().path("containers").path(containerId)
          .path("restart")
          .queryParam("t", String.valueOf(secondsToWaitBeforeRestart));
      request(POST, resource, resource.request());
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }


  @Override
  public void killContainer(final String containerId) throws DockerException, InterruptedException {
    try {
      final WebTarget resource = resource().path("containers").path(containerId).path("kill");
      request(POST, resource, resource.request());
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void stopContainer(final String containerId, final int secondsToWaitBeforeKilling)
      throws DockerException, InterruptedException {
    try {
      final WebTarget resource = noTimeoutResource()
          .path("containers").path(containerId).path("stop")
          .queryParam("t", String.valueOf(secondsToWaitBeforeKilling));
      request(POST, resource, resource.request());
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 304: // already stopped, so we're cool
          return;
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public ContainerExit waitContainer(final String containerId)
      throws DockerException, InterruptedException {
    try {
      final WebTarget resource = noTimeoutResource()
          .path("containers").path(containerId).path("wait");
      // Wait forever
      return request(POST, ContainerExit.class, resource,
                     resource.request(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void removeContainer(final String containerId)
      throws DockerException, InterruptedException {
    removeContainer(containerId, false);
  }

  @Override
  public void removeContainer(final String containerId, final boolean removeVolumes)
      throws DockerException, InterruptedException {
    try {
      final WebTarget resource = resource()
          .path("containers").path(containerId);
      request(DELETE, resource, resource
          .queryParam("v", String.valueOf(removeVolumes))
          .request(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public InputStream exportContainer(String containerId)
      throws DockerException, InterruptedException {
    final WebTarget resource = resource()
        .path("containers").path(containerId).path("export");
    return request(GET, InputStream.class, resource,
                   resource.request(APPLICATION_OCTET_STREAM_TYPE));
  }

  @Override
  public InputStream copyContainer(String containerId, String path)
      throws DockerException, InterruptedException {
    final WebTarget resource = resource()
        .path("containers").path(containerId).path("copy");

    // Internal JSON object; not worth it to create class for this
    JsonNodeFactory nf = JsonNodeFactory.instance;
    final JsonNode params = nf.objectNode().set("Resource", nf.textNode(path));

    return request(POST, InputStream.class, resource,
                   resource.request(APPLICATION_OCTET_STREAM_TYPE),
                   Entity.json(params));
  }

  @Override
  public ContainerInfo inspectContainer(final String containerId)
      throws DockerException, InterruptedException {
    try {
      final WebTarget resource = resource().path("containers").path(containerId).path("json");
      return request(GET, ContainerInfo.class, resource, resource.request(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public ContainerCreation commitContainer(final String containerId,
                                           final String repo,
                                           final String tag,
                                           final ContainerConfig config,
                                           final String comment,
                                           final String author)
      throws DockerException, InterruptedException {

    checkNotNull(containerId, "containerId");
    checkNotNull(repo, "repo");
    checkNotNull(config, "containerConfig");

    WebTarget resource = resource()
        .path("commit")
        .queryParam("container", containerId)
        .queryParam("repo", repo)
        .queryParam("comment", comment);

    if (!isNullOrEmpty(author)) {
      resource = resource.queryParam("author", author);
    }
    if (!isNullOrEmpty(comment)) {
      resource = resource.queryParam("comment", comment);
    }
    if (!isNullOrEmpty(tag)) {
      resource = resource.queryParam("tag", tag);
    }

    log.info("Committing container id: {} to repository: {} with ContainerConfig: {}", containerId,
             repo, config);

    try {
      return request(POST, ContainerCreation.class, resource, resource
          .request(APPLICATION_JSON_TYPE), Entity.json(config));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void pull(final String image) throws DockerException, InterruptedException {
    pull(image, new LoggingPullHandler(image));
  }

  @Override
  public void pull(final String image, final ProgressHandler handler)
      throws DockerException, InterruptedException {
    final ImageRef imageRef = new ImageRef(image);

    WebTarget resource = resource().path("images").path("create");

    resource = resource.queryParam("fromImage", imageRef.getImage());
    if (imageRef.getTag() != null) {
      resource = resource.queryParam("tag", imageRef.getTag());
    }

    try (ProgressStream pull = request(POST, ProgressStream.class, resource,
                                       resource.request(APPLICATION_JSON_TYPE)
                                               .header("X-Registry-Auth", authHeader()))) {
      pull.tail(handler, POST, resource.getUri());
    } catch (IOException e) {
      throw new DockerException(e);
    }
  }

  @Override
  public void push(final String image) throws DockerException, InterruptedException {
    push(image, new LoggingPushHandler(image));
  }

  @Override
  public void push(final String image, final ProgressHandler handler)
      throws DockerException, InterruptedException {
    final ImageRef imageRef = new ImageRef(image);

    WebTarget resource =
        resource().path("images").path(imageRef.getImage()).path("push");

    if (imageRef.getTag() != null) {
      resource = resource.queryParam("tag", imageRef.getTag());
    }

    // the docker daemon requires that the X-Registry-Auth header is specified
    // with a non-empty string even if your registry doesn't use authentication
    try (ProgressStream push =
             request(POST, ProgressStream.class, resource,
                     resource.request(APPLICATION_JSON_TYPE)
                             .header("X-Registry-Auth", authHeader()))) {
      push.tail(handler, POST, resource.getUri());
    } catch (IOException e) {
      throw new DockerException(e);
    }
  }

  @Override
  public void tag(final String image, final String name)
      throws DockerException, InterruptedException {
    final ImageRef imageRef = new ImageRef(name);

    WebTarget resource =
        resource().path("images").path(image).path("tag");

    resource = resource.queryParam("repo", imageRef.getImage());
    if (imageRef.getTag() != null) {
      resource = resource.queryParam("tag", imageRef.getTag());
    }

    try {
      request(POST, resource, resource.request());
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(image, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public String build(final Path directory, final BuildParameter... params)
      throws DockerException, InterruptedException, IOException {
    return build(directory, null, new LoggingBuildHandler(), params);
  }

  @Override
  public String build(final Path directory, final String name, final BuildParameter... params)
      throws DockerException, InterruptedException, IOException {
    return build(directory, name, new LoggingBuildHandler(), params);
  }

  @Override
  public String build(final Path directory, final ProgressHandler handler,
                      final BuildParameter... params)
      throws DockerException, InterruptedException, IOException {
    return build(directory, null, handler, params);
  }

  @Override
  public String build(final Path directory, final String name, final ProgressHandler handler,
                      final BuildParameter... params)
      throws DockerException, InterruptedException, IOException {
    checkNotNull(handler, "handler");

    WebTarget resource = resource().path("build");

    for (final BuildParameter param : params) {
      resource = resource.queryParam(param.queryParam, String.valueOf(param.value));
    }
    if (name != null) {
     resource = resource.queryParam("t", name);
    }

    final File compressedDirectory = CompressedDirectory.create(directory);

    try (ProgressStream build = request(POST, ProgressStream.class, resource,
                                        resource.request(APPLICATION_JSON_TYPE),
                                        Entity.entity(compressedDirectory, "application/tar"))) {
      String imageId = null;
      while (build.hasNextMessage(POST, resource.getUri())) {
        final ProgressMessage message = build.nextMessage(POST, resource.getUri());
        final String id = message.buildImageId();
        if (id != null) {
          imageId = id;
        }
        handler.progress(message);
      }
      return imageId;
    } finally {
      delete(compressedDirectory);
    }
  }

  @Override
  public ImageInfo inspectImage(final String image) throws DockerException, InterruptedException {
    try {
      final WebTarget resource = resource().path("images").path(image).path("json");
      return request(GET, ImageInfo.class, resource, resource.request(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(image, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public List<RemovedImage> removeImage(String image)
      throws DockerException, InterruptedException {
    return removeImage(image, false, false);
  }

  @Override
  public List<RemovedImage> removeImage(String image, boolean force, boolean noPrune)
      throws DockerException, InterruptedException {
    try {
      final WebTarget resource = resource().path("images").path(image)
          .queryParam("force", String.valueOf(force))
          .queryParam("noprune", String.valueOf(noPrune));
      return request(DELETE, REMOVED_IMAGE_LIST, resource, resource.request(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(image);
        default:
          throw e;
      }
    }
  }

  @Override
  public LogStream logs(final String containerId, final LogsParameter... params)
      throws DockerException, InterruptedException {
    WebTarget resource = resource()
        .path("containers").path(containerId).path("logs");

    for (final LogsParameter param : params) {
      resource = resource.queryParam(param.name().toLowerCase(Locale.ROOT), String.valueOf(true));
    }

    try {
      return new LogStream(request(GET, LogReader.class, resource,
                     resource.request("application/vnd.docker.raw-stream")));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId);
        default:
          throw e;
      }
    }
  }

  @Override
  public LogStream attachContainer(final String containerId,
                                   final AttachParameter... params) throws DockerException,
      InterruptedException {
    WebTarget resource = resource().path("containers").path(containerId)
        .path("attach");

    for (final AttachParameter param : params) {
      resource = resource.queryParam(param.name().toLowerCase(Locale.ROOT),
          String.valueOf(true));
    }

    try {
      return new LogStream(request(POST, LogReader.class, resource,
          resource.request("application/vnd.docker.raw-stream")));
    } catch (DockerRequestException e) {
      switch (e.status()) {
      case 404:
        throw new ContainerNotFoundException(containerId);
      default:
        throw e;
      }
    }
  }

  private WebTarget resource() {
    return client.target(uri).path(VERSION);
  }

  private WebTarget noTimeoutResource() {
    return noTimeoutClient.target(uri).path(VERSION);
  }

  private <T> T request(final String method, final GenericType<T> type,
                        final WebTarget resource, final Invocation.Builder request)
      throws DockerException, InterruptedException {
    try {
      return request.async().method(method, type).get();
    } catch (ExecutionException | RuntimeException e) {
      throw propagate(method, resource, e);
    }
  }

  private <T> T request(final String method, final Class<T> clazz,
                        final WebTarget resource, final Invocation.Builder request)
      throws DockerException, InterruptedException {
    try {
      return request.async().method(method, clazz).get();
    } catch (ExecutionException | RuntimeException e) {
      throw propagate(method, resource, e);
    }
  }

  private <T> T request(final String method, final Class<T> clazz,
                        final WebTarget resource, final Invocation.Builder request,
                        final Entity<?> entity)
      throws DockerException, InterruptedException {
    try {
      return request.async().method(method, entity, clazz).get();
    } catch (ExecutionException | RuntimeException e) {
      throw propagate(method, resource, e);
    }
  }

  private void request(final String method,
                       final WebTarget resource,
                       final Invocation.Builder request)
      throws DockerException, InterruptedException {
    try {
      request.async().method(method, String.class).get();
    } catch (ExecutionException | RuntimeException e) {
      throw propagate(method, resource, e);
    }
  }

  private void request(final String method,
                       final WebTarget resource,
                       final Invocation.Builder request,
                       final Entity<?> entity)
      throws DockerException, InterruptedException {
    try {
      request.async().method(method, entity, String.class).get();
    } catch (ExecutionException | RuntimeException e) {
      throw propagate(method, resource, e);
    }
  }

  private RuntimeException propagate(final String method, final WebTarget resource,
                                     final Exception e)
      throws DockerException, InterruptedException {

    Throwable cause = e;

    do {
      Response response;

      if (cause instanceof ResponseProcessingException) {
        response = ((ResponseProcessingException) cause).getResponse();
        throw new DockerRequestException(method, resource.getUri(), response.getStatus(),
            message(response), cause);
      } else if (cause instanceof WebApplicationException) {
        response = ((WebApplicationException) cause).getResponse();
        throw new DockerRequestException(method, resource.getUri(), response.getStatus(),
            message(response), cause);
      } else if ((cause instanceof SocketTimeoutException)
          || (cause instanceof ConnectTimeoutException)) {
        throw new DockerTimeoutException(method, uri, cause);
      } else if ((cause instanceof InterruptedException)
          || (cause instanceof InterruptedIOException)) {
        throw new InterruptedException("Interrupted: " + method + " " + resource);
      }
    } while ((cause = cause.getCause()) != null);

    throw new DockerException(e);
  }

  private String message(final Response response) {
    final Readable reader = new InputStreamReader(response.readEntity(InputStream.class), UTF_8);
    try {
      return CharStreams.toString(reader);
    } catch (IOException ignore) {
      return null;
    }
  }
  
  private String authHeader() throws DockerException {
    if (authConfig == null) {
      return "null";
    }
    try {
      return Base64.getEncoder().encodeToString(
          ObjectMapperProvider.objectMapper().writeValueAsString(authConfig).getBytes());
    } catch (JsonProcessingException ex) {
      throw new DockerException("Could not encode X-Registry-Auth header", ex);
    }
  }

  /**
   * Create a new {@link DefaultDockerClient} builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Create a new {@link DefaultDockerClient} builder prepopulated with values loaded
   * from the DOCKER_HOST and DOCKER_CERT_PATH environment variables.
   * @return Returns a builder that can be used to further customize and then build the client.
   * @throws DockerCertificateException
   */
  public static Builder fromEnv() throws DockerCertificateException {
    final String endpoint = fromNullable(getenv("DOCKER_HOST")).or(defaultEndpoint());
    final String dockerCertPath = getenv("DOCKER_CERT_PATH");

    final Builder builder = new Builder();

    if (endpoint.startsWith(UNIX_SCHEME + "://")) {
      builder.uri(endpoint);
    } else {
      final String stripped = endpoint.replaceAll(".*://", "");
      final HostAndPort hostAndPort = HostAndPort.fromString(stripped);
      final String hostText = hostAndPort.getHostText();
      final String scheme = isNullOrEmpty(dockerCertPath) ? "http" : "https";

      final int port = hostAndPort.getPortOrDefault(DEFAULT_PORT);
      final String address = isNullOrEmpty(hostText) ? DEFAULT_HOST : hostText;

      builder.uri(scheme + "://" + address + ":" + port);
    }

    if (!isNullOrEmpty(dockerCertPath)) {
      builder.dockerCertificates(new DockerCertificates(Paths.get(dockerCertPath)));
    }

    return builder;
  }

  private static String defaultEndpoint() {
    if (getProperty("os.name").equalsIgnoreCase("linux")) {
      return DEFAULT_UNIX_ENDPOINT;
    } else {
      return DEFAULT_HOST + ":" + DEFAULT_PORT;
    }
  }
  
  public static class Builder {

    private URI uri;
    private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;
    private long readTimeoutMillis = DEFAULT_READ_TIMEOUT_MILLIS;
    private int connectionPoolSize = DEFAULT_CONNECTION_POOL_SIZE;
    private DockerCertificates dockerCertificates;
    private AuthConfig authConfig;

    public URI uri() {
      return uri;
    }

    public Builder uri(final URI uri) {
      this.uri = uri;
      return this;
    }

    /**
     * Set the URI for connections to Docker.
     */
    public Builder uri(final String uri) {
      return uri(URI.create(uri));
    }

    public long connectTimeoutMillis() {
      return connectTimeoutMillis;
    }

    /**
     * Set the timeout in milliseconds until a connection to Docker is established.
     * A timeout value of zero is interpreted as an infinite timeout.
     */
    public Builder connectTimeoutMillis(final long connectTimeoutMillis) {
      this.connectTimeoutMillis = connectTimeoutMillis;
      return this;
    }

    public long readTimeoutMillis() {
      return readTimeoutMillis;
    }

    /**
     * Set the SO_TIMEOUT in milliseconds. This is the maximum period of inactivity
     * between receiving two consecutive data packets from Docker.
     */
    public Builder readTimeoutMillis(final long readTimeoutMillis) {
      this.readTimeoutMillis = readTimeoutMillis;
      return this;
    }

    public DockerCertificates dockerCertificates() {
      return dockerCertificates;
    }

    /**
     * Provide certificates to secure the connection to Docker.
     */
    public Builder dockerCertificates(final DockerCertificates dockerCertificates) {
      this.dockerCertificates = dockerCertificates;
      return this;
    }

    public int connectionPoolSize() {
      return connectionPoolSize;
    }

    /**
     * Set the size of the connection pool for connections to Docker. Note that due to
     * a known issue, DefaultDockerClient maintains two separate connection pools, each
     * of which is capped at this size. Therefore, the maximum number of concurrent
     * connections to Docker may be up to 2 * connectionPoolSize.
     */
    public Builder connectionPoolSize(int connectionPoolSize) {
      this.connectionPoolSize = connectionPoolSize;
      return this;
    }

    public AuthConfig authConfig() {
      return authConfig;
    }

    /**
     * Set the auth parameters for pull/push requests from/to private repositories.
     */
    public Builder authConfig(AuthConfig authConfig) {
      this.authConfig = authConfig;
      return this;
    }

    public DefaultDockerClient build() {
      return new DefaultDockerClient(this);
    }
  }
}
