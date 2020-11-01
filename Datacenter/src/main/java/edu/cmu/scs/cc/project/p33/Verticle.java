package edu.cmu.scs.cc.project.p33;

import java.time.Instant;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public class Verticle extends AbstractVerticle {
    /**
     * Logger
     */
    private static final Logger LOGGER = Logger.getLogger(KeyValueStore.class);

    public static void main(String[] args) {
        // disable netty logging
        Logger.getLogger("io.netty").setLevel(Level.WARN);
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Verticle(), ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Datacenter Server is deployed at " + Instant.now().toEpochMilli());
            } else {
                LOGGER.error("Datacenter Server failed to deploy!", ar.cause());
            }
        });
    }
    
    @Override
    public void start() throws Exception {
        final Router router = Router.router(vertx);
        KeyValueStore keyValueStore = new KeyValueStore();
        router.route("/get").handler(keyValueStore::getHandler);
        router.route("/put").handler(keyValueStore::putHandler);
        router.route("/flush").handler(keyValueStore::flushHandler);
        router.route("/precommit").handler(keyValueStore::precommitHandler);
        router.route("/test").handler(keyValueStore::testHandler);
        router.route().handler(keyValueStore::noMatchHandler);
        vertx.createHttpServer().requestHandler(req -> router.accept(req)).listen(8080);
    }
}
