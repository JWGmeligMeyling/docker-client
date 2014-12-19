package com.spotify.docker.client;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.spotify.docker.client.DockerClient.AttachParameter;
import com.spotify.docker.client.DockerClient.LogsParameter;
import com.spotify.docker.test.CreateContainer;
import com.spotify.docker.test.DockerContainer;

public class DockerAttachTest {
  
  private static DockerClient dockerClient;
  private static ExecutorService executor;
  
  @Rule
  public DockerContainer dockerContainer = new DockerContainer(dockerClient);
  
  @BeforeClass
  public static void setUp() throws DockerCertificateException {
    dockerClient = DefaultDockerClient.fromEnv().readTimeoutMillis(120000).build();
    executor = Executors.newSingleThreadExecutor();
  }
  
  @Test
  @CreateContainer(image = "busybox", command = {"sh", "-c", "echo \"test\""}, start = true)
  public void testIt() throws IOException,
      DockerException, InterruptedException {
    
    String containerId = dockerContainer.getContainerId();
    dockerClient.waitContainer(containerId);
    try(LogStream logStream = dockerClient.logs(containerId, LogsParameter.STDOUT)) {
      assertThat(logStream.readFully(), equalTo("test\n"));
    }
  }
  
  @Test
  @CreateContainer(image = "busybox", command = {"sh", "-c", "echo \"stdout\" && >&2 echo \"stderr\""}, start = true)
  public void testAttachLog() throws IOException,
      DockerException, InterruptedException {
    
    final String containerId = dockerContainer.getContainerId();

    final PipedInputStream stdout = new PipedInputStream();
    final PipedInputStream stderr = new PipedInputStream();
    final PipedOutputStream stdout_pipe = new PipedOutputStream(stdout);
    final PipedOutputStream stderr_pipe = new PipedOutputStream(stderr);
    
    executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        dockerClient
          .attachContainer(containerId,
              AttachParameter.LOGS, AttachParameter.STDOUT,
              AttachParameter.STDERR, AttachParameter.STREAM)
          .attach(stdout_pipe, stderr_pipe);
        return null;
      }
      
    });
    
    try (Scanner sc_stdout = new Scanner(stdout);
        Scanner sc_stderr = new Scanner(stderr)) {
      assertThat(sc_stdout.next(), equalTo("stdout"));
      assertThat(sc_stderr.next(), equalTo("stderr"));
    }
  }

  @AfterClass
  public static void cleanUp() {
    dockerClient.close();
    executor.shutdownNow();
  }
  
}
