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

import org.apache.http.annotation.Immutable;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.params.HttpParams;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * Provides a ConnectionSocketFactory for connecting Apache HTTP clients to Unix sockets.
 */
@Immutable
public class UnixConnectionSocketFactory implements SchemeSocketFactory {

  private File socketFile;

  public UnixConnectionSocketFactory(final URI socketUri) {
    super();

    final String filename = socketUri.toString()
        .replaceAll("^unix:///", "unix://localhost/")
        .replaceAll("^unix://localhost", "");

    this.socketFile = new File(filename);
  }

  public static URI sanitizeUri(final URI uri) {
    if (uri.getScheme().equals("unix")) {
      return URI.create("unix://localhost:80");
    } else {
      return uri;
    }
  }

  @Override
  public boolean isSecure(Socket sock) throws IllegalArgumentException {
    return false;
  }

  @Override
  public Socket createSocket(HttpParams params) throws IOException {
    return new ApacheUnixSocket();
  }

  @Override
  public Socket connectSocket(Socket sock, InetSocketAddress remoteAddress,
      InetSocketAddress localAddress, HttpParams params) throws IOException, UnknownHostException,
      ConnectTimeoutException {
    try {
      sock.connect(new AFUNIXSocketAddress(socketFile));
    } catch (SocketTimeoutException e) {
      throw new ConnectTimeoutException();
    }

    return sock;
  }
}
