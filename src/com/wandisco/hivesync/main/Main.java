package com.wandisco.hivesync.main;

import java.net.URI;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import com.beust.jcommander.JCommander;
import com.wandisco.hivesync.rest.TableResource;

/**
 * 
 * @author Oleg Danilov
 * 
 */
public class Main {

  private static Params p;

  public static void main(String[] args) {
    p = new Params();
    JCommander jce = new JCommander(p, args);
    try {
      System.out.println("\"Hello World\" Jersey Example App");

      final ResourceConfig resourceConfig = new ResourceConfig(TableResource.class);
      final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(new URI("http://localhost:"+p.getPort()+"/"), resourceConfig, false);
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          server.shutdownNow();
        }
      }));
      server.start();

      Thread.currentThread().join();
    } catch (Exception ex) {
    }

  }

  // public static void main(String[] args) throws Exception {
  // p = new Params();
  // JCommander jce = new JCommander(p, args);
  //
  // if (p.getHelp() != null) {
  // jce.setProgramName("hive-metastore-sync");
  // jce.usage();
  // return;
  // }
  //
  // // Delete dryRun file
  // String dryRunFile = p.getDryRunFile();
  // Commands.setDryRunFile(dryRunFile);
  // if (dryRunFile != null) {
  // (new File(dryRunFile)).delete();
  // }
  //
  // HiveSync hs = new HiveSync(p.getSrc(), p.getSrcUser(), p.getSrcPass(),
  // p.getDst(), p.getDstUser(),
  // p.getDstPass(), p.getDatabases());
  // hs.execute();
  // }

}
