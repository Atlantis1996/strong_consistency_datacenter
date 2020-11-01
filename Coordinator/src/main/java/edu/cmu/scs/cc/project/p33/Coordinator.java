package edu.cmu.scs.cc.project.p33;

import java.io.BufferedReader;
// import java.io.FileInputStream;
// import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
/**
 * We will describe a viable implementation of this task.
 * You may decide whether you want to strictly follow our suggestions or not.
 * 
 * Hints:
 * In Task 1, you may have implemented a locking mechanism on the Task 1's Coordinator.java.
 * In Task 2, however, the locking mechanism should be implemented in
 * {@code KeyValueStore.java} on the data center instances. The major
 * responsibility of the coordinator is to send out PRECOMMIT/PUT/GET requests
 * to one or all data centers.
 *
 * One possible viable implementation of Task 2 does not require any additional data structures to
 * be added into this {@link Coordinator} class.
 *
 * A reminder that you need to finish all the TODOs. You are required to remove them
 * in your last submission, as an indication of task completion and to show that you
 * are able to follow a sound coding discipline and best practices.
 */
public class Coordinator {

    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    /**
     * This variable indicates what region this Coordinator is in.
     * This variable will have a different value (passed during the runtime automatically)
     * according to the region.
     *
     * The mapping is as follows:
     * Project 3.3 US-EAST Coordinator (Task 2): 1
     * Project 3.3 US-WEST Coordinator (Task 2): 2
     * Project 3.3 APAC-SG Coordinator (Task 2): 3
     *
     * You should NOT modify this value, instead
     * please use it as it is in {@link #getHandler(RoutingContext)}.
     */
    private static int region = KeyValueLib.region;

    /**
     * Default mode: Strongly consistent.
     *
     * Options: strong, eventual.
     *
     * You are NOT required to handle corner cases, e.g., when the value
     * of this variable is neither strong nor eventual.
     */
    private static String consistencyType = "strong";

    /**
     * These PRIVATE-IPs will be filled by {@link #Coordinator()} constructor. You don't need
     * to fill them manually.
     */
    private static String coordinatorUSE = "<COORDINATOR_USE-PRIVATE-IP>";
    private static String coordinatorUSW = "<COORDINATOR_USW-PRIVATE-IP>";
    private static String coordinatorSING = "<COORDINATOR_SING-PRIVATE-IP>";
    private static String dataCenterUSE = "<DATACENTER_USE-PRIVATE-IP>";
    private static String dataCenterUSW = "<DATACENTER_USW-PRIVATE-IP>";
    private static String dataCenterSING = "<DATACENTER_SING-PRIVATE-IP>";
    private static String truetimeServer = "<TRUETIMESERVER-PRIVATE-IP>";
    
    
    /**
     * Initializes a Coordinator object.
     */
    public Coordinator() {
        // Read the private IPs from config.prop
        Properties properties = null;
        try {
            InputStream in = getClass().getClassLoader().getResourceAsStream("config.prop");
            if (in == null) {
                LOGGER.error("config.prop file isn't found!!");
            } else {
                properties = new Properties();
                properties.load(in);
                in.close();
            }
        } catch (IOException e) {
            LOGGER.error("IOException while parsing config.prop!");
        }

        if (properties != null) {
            if (!(properties.containsKey("COORDINATOR_USE") &&
                    properties.containsKey("COORDINATOR_USW") &&
                    properties.containsKey("COORDINATOR_SING") &&
                    properties.containsKey("DATACENTER_USE") &&
                    properties.containsKey("DATACENTER_USW") &&
                    properties.containsKey("DATACENTER_SING") &&
                    properties.containsKey("TRUETIMESERVER"))) {
                LOGGER.error("Failed to read private IPs. Please make sure config.prop has proper format.");
            }

            // Initialize the local variables
            coordinatorUSE = properties.getProperty("COORDINATOR_USE");
            coordinatorUSW = properties.getProperty("COORDINATOR_USW");
            coordinatorSING = properties.getProperty("COORDINATOR_SING");
            dataCenterUSE = properties.getProperty("DATACENTER_USE");
            dataCenterUSW = properties.getProperty("DATACENTER_USW");
            dataCenterSING = properties.getProperty("DATACENTER_SING");
            truetimeServer = properties.getProperty("TRUETIMESERVER");

            // Initialize the KeyValueLib with private IPs
            KeyValueLib.dataCenters.put(dataCenterUSE, 1);
            KeyValueLib.dataCenters.put(dataCenterUSW, 2);
            KeyValueLib.dataCenters.put(dataCenterSING, 3);
            KeyValueLib.coordinators.put(coordinatorUSE, 1);
            KeyValueLib.coordinators.put(coordinatorUSW, 2);
            KeyValueLib.coordinators.put(coordinatorSING, 3);
        }
    }

    /**
     * TODO: Implement the PUT handler.
     *
     * Each PUT operation is handled in a new thread.
     *
     * Hint: one possible viable implementation of this handler is as follows:
     * 1. When this Coordinator instance receives a PUT request,
     * immediately send out a PRECOMMIT request to each data center concurrently.
     * 2. Wait until all the blocking PRECOMMIT requests finish.
     * 3. Send out a PUT request to each data center concurrently.
     * 4. Wait until all the blocking PUT requests finish.
     * 5. Return an empty response to the client,
     * i.e., using {@code context.response().end()}.
     * 
     * @param context the context for the handling of a request
     */
    public void putHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        final String value = context.request().getParam("value");
        Thread t = new Thread(new Runnable() {
            public void run() {
                // You should use the following timestamp
                Long timestamp = getTimeStamp(context);
                // WARNING: Do not modify the line above

                try {

                    // TODO: program here
 

                    //2. Wait until all the blocking PRECOMMIT requests finish.
                    ArrayList<Thread> threads = new ArrayList<>();

                    for (int i = 1; i < 4; i++) {
                        // if (i != region) {
                            final String dC = getDataCenter2(i);
                            Thread t = new Thread(new Runnable() {
                                public void run() {
                                    // Do something in the new thread
                                    try {
                                        KeyValueLib.PRECOMMIT(dC, key, timestamp);
                                    } catch (Exception e) {
                                        //
                                    }
                                }
                            });
                            t.start();
                            threads.add(t);
                        // }

                    }
                    // Wait for all launched threads to finish
                    for (int i = 0; i < 3; i++) {
                        try {
                            threads.get(i).join();
                        } catch (InterruptedException e) {
                            // Catch exception if thread is interrupted;
                            LOGGER.error("error message");
                        }
                    }

                    //3. Send out a PUT request to each data center concurrently.
                    threads = new ArrayList<>();

                    for (int i = 1; i < 4; i++) {

                        final String dc = getDataCenter2(i);
                        Thread t = new Thread(new Runnable() {
                            public void run() {
                                // Do something in the new thread
                                try {
                                    KeyValueLib.PUT(dc, key, value, timestamp, consistencyType);
                                    LOGGER.error(dc);
                                } catch (Exception e) {
                                    //
                                }
                            }
                        });
                        t.start();
                        threads.add(t);
                    }
                    // Wait for all launched threads to finish
                    for (int i = 0; i < 3; i++) {
                        try {
                            threads.get(i).join();
                        } catch (InterruptedException e) {
                            // Catch exception if thread is interrupted;
                            LOGGER.error("error message");
                        }
                    }

                } catch (Exception e) {
                    LOGGER.error("PutHandler Exception: " + e.getMessage());
                } finally {
                    // WARNING: Do not remove the line below
                    context.response().end();
                }
            }
        });
        t.start();
    }

    /**
     * TODO: Implement the GET handler.
     *
     * Each operation is handled in a new thread.
     *
     * Hint: one possible viable implementation of this handler is as follows:
     * 1. When this Coordinator instance receives a GET request,
     * immediately send out a GET request to the data center using KeyValueLib
     * at the same region as per the value of {@link #region}.
     *
     * 2. Once the data center returns the response, pass the value back
     * to the client, i.e., using {@code context.response().end(value)}.
     * 
     * @param context the context for the handling of a request
     */
    public void getHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        Thread t = new Thread(new Runnable() {
            public void run() {
                // You should use the following variable to store your value
                String value = "null";
                // You should use the following timestamp
                Long timestamp = getTimeStamp(context);
                // WARNING: Do not modify the line above

                try {
                    
                    // TODO: program here
                    final String dataCenter = getDataCenter2(region); 
                    LOGGER.error("1"); 
                    value = KeyValueLib.GET(dataCenter, key, timestamp, consistencyType);
                    LOGGER.error("2"); 
                } catch (Exception e) {
                    LOGGER.error("GetHandler Exception: " + e.getMessage());
                } finally {
                    // WARNING: Do not remove the line below
                    context.response().end(value);
                }
            }
        });
        t.start();
    }

    public String getDataCenter(String loc) {
        if (loc.equals("1")) {
            return dataCenterUSE;
        }
        if (loc.equals("2")) {
            return dataCenterUSW;
        }
        if (loc.equals("3")) {
            return dataCenterSING;
        }
        return "";
    }

    public String getDataCenter2(int i) {
        if (i == 1) {
            return dataCenterUSE;
        }
        if (i == 2) {
            return dataCenterUSW;
        }
        if (i == 3) {
            return dataCenterSING;
        }
        return "";
    }
    /**
     * This handler is used by the grader to change the consistency level.
     * 
     * Note: DO NOT modify this method.
     * 
     * @param context the context for the handling of a request
     */
    public void consistencyHandler(RoutingContext context) {
        consistencyType = context.request().getParam("consistency");
        context.response().end();
    }

    /**
     * This endpoint will be used by the auto-grader to flush all the data centers
     * before tests.
     *
     * One possible viable implementation of Task 2 does NOT require
     * any modification of this method.
     * 
     * @param context the context for the handling of a request
     */
    public void flushHandler(RoutingContext context) {
        try {
            flush(dataCenterUSE);
            flush(dataCenterUSW);
            flush(dataCenterSING);
        } catch (Exception e) {
            LOGGER.error("flushHandler exception!", e);
        }
        context.response().end();
    }

    /**
     * This method flushes a data center by calling its flush endpoint.
     * 
     * Note: DO NOT modify this method.
     *
     * @param dataCenter the private ip of data center.
     * @throws Exception
     */
    private void flush(String dataCenter) throws Exception {
        URL url = new URL("http://" + dataCenter + ":8080/flush");
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openConnection().getInputStream()));
        // Wait for the data center to finish flush
        while (in.readLine() != null) {
            // do nothing
        }
        in.close();
    }

    /**
     * Test if this server is running.
     * 
     * Note: DO NOT modify this method.
     * 
     * @param context the context for the handling of a request
     */
    public void testHandler(RoutingContext context) {
        String response = "Coordinator is up and running";
        context.response().putHeader("Content-Type", "text/plain");
        context.response().putHeader("Content-Length", String.valueOf(response.length()));
        context.response().end(response);
        context.response().close();
    }

    /**
     * No match handler.
     * 
     * Note: DO NOT modify this method.
     * 
     * @param context the context for the handling of a request
     */
    public void noMatchHandler(RoutingContext context) {
        context.response().putHeader("Content-Type", "text/html");
        String response = "Not found.";
        context.response().putHeader("Content-Length", String.valueOf(response.length()));
        context.response().end(response);
        context.response().close();
    }

    /**
     * Assign a timestamp to a request.
     * 
     * Note: DO NOT modify this method.
     * 
     * @param context
     * @return the Timestamp retrieved from the truetime server
     */
    public Long getTimeStamp(RoutingContext context) {
        try {
            return KeyValueLib.GETTIME(truetimeServer, context);
        } catch (IOException e) {
            LOGGER.error("Failed to retrieve timestamp.", e);
            return -1L;
        }
    }
}
