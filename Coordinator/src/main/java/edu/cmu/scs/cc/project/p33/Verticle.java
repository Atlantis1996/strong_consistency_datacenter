package edu.cmu.scs.cc.project.p33;

import java.time.Instant;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public class Verticle extends AbstractVerticle {

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    public static void main(String[] args) {
        // disable netty logging
        Logger.getLogger("io.netty").setLevel(Level.WARN);
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Verticle(), ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Coordinator Server is deployed at " + Instant.now().toEpochMilli());
            } else {
                LOGGER.error("Coordinator Server failed to deploy!", ar.cause());
            }
        });
    }

    @Override
    public void start() throws Exception {
        final Router router = Router.router(vertx);
        Coordinator coordinator = new Coordinator();
        router.route("/get").handler(coordinator::getHandler);
        router.route("/put").handler(coordinator::putHandler);
        router.route("/consistency").handler(coordinator::consistencyHandler);
        router.route("/flush").handler(coordinator::flushHandler);
        router.route("/test").handler(coordinator::testHandler);
        router.route().handler(coordinator::noMatchHandler);
        vertx.createHttpServer().requestHandler(req -> router.accept(req)).listen(8080);
    }
}
