package edu.cmu.scs.cc.project.p33;

import org.apache.log4j.Logger;

import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
// import java.util.ArrayList;
/**
 * We will describe a possible viable implementation of this task.
 * You may decide whether you want to strictly follow our suggestions or not.
 * Hints:
 * In Task 1, you may have implemented a locking mechanism in Task 1's Coordinator.java.
 * In Task 2, however, the locking mechanism should be implemented in
 * in this {@link KeyValueStore} class running on the data center instances.
 *
 * This {@link KeyValueStore} will deal with the locking and the operation ordering by per key.
 *
 * One possible viable implementation of Task 2 requires you to add additional data structures
 * into this {@link KeyValueStore} class. For example, one of the data structures
 * can be a map as an in-memory key-value store, such as a ConcurrentHashMap.
 * You may need more data structures for the purpose of locking and operation ordering.
 * All the data structures you added, should be initialized or re-initialized in two places:
 * 1. {@link #KeyValueStore()}
 * 2. {@link #flushHandler(RoutingContext)}
 *
 * A reminder that you need to finish all the TODOs and you are required to remove them
 * in your last submission. This will indicate task completion and show that you are
 * able to follow a sound coding discipline and best practices.
 */
public class KeyValueStore {

    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getLogger(KeyValueStore.class);
    
    /**
     * TODO: define your own data structures here
     */
    private static String dataCenter = "DATACENTER-PRIVATE_IP";
    ConcurrentHashMap<String, ReadWriteLock> map;
    ConcurrentHashMap<String, String> store;
    Long lastTimestamp;
    public class ReadWriteLock {

        private PriorityBlockingQueue<Long> getQueue = new PriorityBlockingQueue<>();
        private PriorityBlockingQueue<Long> putQueue = new PriorityBlockingQueue<>();
        // private PriorityBlockingQueue<Long> precommitQueue = new PriorityBlockingQueue<>();
        
        // Use synchronized method so that these functions will not be executed concurrently.
        // This is the reason why we don't need to use AtomicInteger above
        public synchronized void lockRead(Long timestamp) throws InterruptedException {
            getQueue.add(timestamp);
            while (putQueue.size() > 0 && putQueue.peek().compareTo(getQueue.peek()) < 0) {
                // While there is any writer holding the lock or pending write requests in the queue
                wait();
            }
        }

        public synchronized void unlockRead(Long timestamp) {
            getQueue.remove(timestamp);
            notifyAll(); // It will notify all waiting threads to check and see if they can run
        }

        public synchronized void lockWrite(Long timestamp) throws InterruptedException {
            // putQueue.add(timestamp);
            while (!putQueue.peek().equals(timestamp) || (getQueue.size() > 0 && getQueue.peek().compareTo(putQueue.peek()) < 0)) {
                wait();
            }
        }

        public synchronized void unlockWrite(Long timestamp) throws InterruptedException {
            putQueue.remove(timestamp);
            notifyAll();
        }

        public synchronized void addPrecommit(Long timestamp) throws InterruptedException {
            // precommitQueue.add(timestamp);
            putQueue.add(timestamp);
            // precommitQueue.remove(timestamp);
            notifyAll();
        }

        // public synchronized void unlockPrecommit(Long timestamp) throws InterruptedException {
        //     precommitQueue.remove(timestamp);
        // }
    }
    /**
     * TODO: initialize the data structures
     */
    public KeyValueStore() {
        map = new ConcurrentHashMap<>();
        store = new ConcurrentHashMap<>();
        lastTimestamp = 1000L;
    }

    /**
     * TODO: Implement the PRECOMMIT handler.
     *
     * Hint: one possible viable implementation of this handler is as follows:
     *
     * 1. Make this PRECOMMIT operation block all the other GET/PUT operations
     * with a larger timestamp.
     *
     * @param context the context for the handling of a request
     */
    public void precommitHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        final long timestamp = Long.valueOf(context.request().getParam("timestamp"));

        try {

            // TODO: Write your program here
            map.putIfAbsent(key, new ReadWriteLock());
            final ReadWriteLock readWriteLock = map.get(key);
            readWriteLock.addPrecommit(timestamp);
            // readWriteLock.unlockPrecommit(timestamp);

        } catch (Exception e) {
            LOGGER.error("precommitHandler Exception: " + e.getMessage());
        } finally {
            /* Do not remove the following lines */
            context.response().putHeader("Content-Type", "text/plain");
            context.response().end("stored");
            context.response().close();
        }
    }

    /**
     * TODO: Implement the PUT handler.
     *
     * Each PUT operation is handled in a new thread.
     *
     * Note: Its corresponding PRECOMMIT operation (i.e., which shares the same timestamp)
     * should have already been queued, as no PUT operation can be sent from
     * a coordinator before all the corresponding PRECOMMIT requests finish.
     * 
     * Hint: one possible viable implementation of this handler is as follows:
     *
     * 1. Attempt to block all other operations with a larger timestamp.
     * The thread may need to wait until its corresponding PRECOMMIT operation
     * is at the head of the queue.
     * 2. Update the in-memory key-value store.
     * 3. Stop blocking other PUT/GET operations.
     *
     * @param context the context for the handling of a request
     */
    public void putHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        final String value = context.request().getParam("value");
        final String consistency = context.request().getParam("consistency");
        final long timestamp = Long.valueOf(context.request().getParam("timestamp"));

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (consistency.equals("strong")) {
                        map.putIfAbsent(key, new ReadWriteLock());
                        final ReadWriteLock readWriteLock = map.get(key);
                        readWriteLock.lockWrite(timestamp);
                        store.put(key, value);
                        LOGGER.error(store.get(key));
                        readWriteLock.unlockWrite(timestamp);
                    } else {
                        if (timestamp > lastTimestamp) {
                                lastTimestamp = timestamp;
                                store.put(key, value);
                        }
                    }
                    // TODO: Write your program here

                } catch (Exception e) {
                    LOGGER.error("putHandler Exception: " + e.getMessage());
                } finally {
                    // Do not remove this
                    String response = "stored";
                    context.response().putHeader("Content-Type", "text/plain");
                    context.response().putHeader("Content-Length", String.valueOf(response.length()));
                    context.response().end(response);
                    context.response().close();
                }
            }
        });
        thread.start();
    }

    /**
     * TODO: Implement the GET handler.
     *
     * Each GET operation is handled in a new thread.
     *
     * Hint: one possible viable implementation of this handler is as follows:
     *
     * 1. Attempt to block all the other PUT operations with a larger timestamp.
     * The thread may need to wait if this GET operation is blocked by other
     * PRECOMMIT/PUT operations.
     * 2. Get the value from the in-memory key-value store.
     * 3. Stop blocking other PUT operations.
     * 4. Pass the value back to the coordinator, i.e., using {@code context.response().end(value)}.
     *
     * @param context the context for the handling of a request
     */
    public void getHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        final String consistency = context.request().getParam("consistency");
        final long timestamp = Long.valueOf(context.request().getParam("timestamp"));

        Thread thread = new Thread(new Runnable() {
            public void run() {
                String value = "null";
                try {

                    // TODO: Write your program here
                    map.putIfAbsent(key, new ReadWriteLock());  
                    final ReadWriteLock readWriteLock = map.get(key);  
                    readWriteLock.lockRead(timestamp); 
                    LOGGER.error("01"); 
                    value = store.get(key); 
                    LOGGER.error("02"); 
                    readWriteLock.unlockRead(timestamp); 
                    LOGGER.error("03");   
                    LOGGER.error(value);   

                    context.response().putHeader("Content-Type", "text/plain");
                    LOGGER.error("04");   
                    context.response().putHeader("Content-Length", String.valueOf(value.length()));
                    LOGGER.error("05");   
                    context.response().end(value);
                    LOGGER.error("06");   

                } catch (Exception e) {
                    LOGGER.error("getHandler Exception: " + e.getMessage());
                } finally {
                    context.response().close();
                }
            }
        });
        thread.start();
    }

    /**
     * TODO: re-initialize the data structures.
     * For example, clean up the values associated to the keys.
     * 
     * @param context the context for the handling of a request
     */
    public void flushHandler(RoutingContext context) {
        /*
         * TODO: Add code to here to flush your datastore. This is MANDATORY.
         */

        map = new ConcurrentHashMap<>();
        store = new ConcurrentHashMap<>();
        lastTimestamp = 1000L;
        context.response().putHeader("Content-Type", "text/plain");
        context.response().end();
        context.response().close();
    }

    /**
     * Test handler: test if this server is running.
     * 
     * Note: DO NOT modify this method.
     * 
     * @param context the context for the handling of a request
     */
    public void testHandler(RoutingContext context) {
        String response = "Datacenter is up and running";
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
}
