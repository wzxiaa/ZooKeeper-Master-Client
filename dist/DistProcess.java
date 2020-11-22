import java.io.*;

import java.util.*;
import java.util.concurrent.*;
// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// TODO
// Replace 24 with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess { // implements Watcher, AsyncCallback.ChildrenCallback,
                           // AsyncCallback.StatCallback
    // AsyncCallback.StatCallback
    // AsyncCallback.StatCallback
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";

    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isMaster = false;
    boolean idle = false;

    ConcurrentLinkedQueue<String> task_queue = new ConcurrentLinkedQueue<String>();
    ConcurrentHashMap<String, String> workers_status = new ConcurrentHashMap<String, String>();
    HashSet<String> visited_children = new HashSet<String>();

    public String getIdleWorker(ConcurrentHashMap<String, String> map) {
        String id = "";
        Iterator it = map.entrySet().iterator();
        printBlue("[Master worker_status]: " + workers_status);
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            if (pair.getValue().equals("idle")) {
                id = (String) pair.getKey();
                printBlue("[Master getIdleWorker]: Master found an idle worker : " + workers_status);
                return id;
            }
        }
        printRed("[Master getIdleWorker] : No worker available");
        return id;
    }

    public void printBlue(String str) {
        System.out.println(ANSI_BLUE + str + ANSI_RESET);
    }

    public void printYellow(String str) {
        System.out.println(ANSI_YELLOW + str + ANSI_RESET);
    }

    public void printRed(String str) {
        System.out.println(ANSI_RED + str + ANSI_RESET);
    }

    // Master deal with new tasks submitted by the client
    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist24/tasks")) {
                // printRed("[Master newTaskWatcher]: master is notified by a new task
                // submitted");
                getNewlySubmittedTasks();
            }
        }
    };

    AsyncCallback.ChildrenCallback newTaskCallBack = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            for (String c : children) {
                if (!visited_children.contains(c)) {
                    printYellow("---------------------- NEW TASK [Master] ----------------------");
                    printRed("[Master newTaskCallBack]: master finds a new TASK submitted: " + c);
                    visited_children.add(c);
                    String worker_id = getIdleWorker(workers_status);
                    try {
                        if (worker_id.equals("")) { // no
                            // available
                            // worker
                            // right
                            // now
                            printBlue("[Master newTaskCallBack]: no idle worker available right now");
                            task_queue.add(c); // add the task into
                                               // the task queue
                            printBlue("[Master newTaskCallBack]: Adding " + c + " into the task queue");
                            byte[] taskSerial = zk.getData("/dist24/tasks/" + c, false, null);
                            // assignTask(worker_id, c, taskSerial);

                        } else {
                            workers_status.put(worker_id, c);
                            byte[] taskSerial = zk.getData("/dist24/tasks/" + c, false, null);
                            assignTask(worker_id, c, taskSerial);
                            watchOnAssignedWorkerTask(worker_id, c);
                        }
                    } catch (NodeExistsException nee) {
                        System.out.println(nee);
                    } catch (KeeperException ke) {
                        System.out.println(ke);
                    } catch (InterruptedException ie) {
                        System.out.println(ie);
                    }
                } else {
                    // printRed("[Master newTaskCallBack]: " + c + " is already in the master task
                    // list");
                }
            }
        }
    };

    void getNewlySubmittedTasks() {
        zk.getChildren("/dist24/tasks", newTaskWatcher, newTaskCallBack, null);
    }

    // One time trigger for master to watch on the worker
    void watchOnAssignedWorkerTask(String worker_id, String task_id) {
        zk.exists("/dist24/workers/" + worker_id + "/" + task_id, null, workerTaskFinishedCallBack, null);
    }

    // new WorkerTaskDeletionWatch(worker_id, task_id)

    /**
     * The customer watcher implementation to be installed to watch for worker task
     * deletion
     */
    // class WorkerTaskDeletionWatch implements Watcher {
    // private String worker_id;
    // private String task_id;

    // public WorkerTaskDeletionWatch(String worker_id, String task_id) {
    // this.worker_id = worker_id;
    // this.task_id = task_id;
    // }

    // public void process(WatchedEvent e) {
    // System.out.println("[Master WorkerTaskDeletionWatch]: " + e);
    // if (e.getType() == Watcher.Event.EventType.NodeDeleted
    // && e.getPath().equals("/dist24/workers/" + worker_id + "/" + task_id)) {
    // printRed("[Master newWorkerWatcher]: master is notified that " + worker_id +
    // " has finished task "
    // + task_id);
    // }
    // }
    // }

    AsyncCallback.StatCallback workerTaskFinishedCallBack = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            String[] tokens = path.split("/");
            String worker_id = tokens[3];
            String task_id = tokens[4];
            switch (Code.get(rc)) {
                case NONODE:
                    printBlue("[Matser workerTaskFinishedCallBack]: Master detects " + worker_id + " finished "
                            + task_id);
                    workers_status.put(worker_id, "idle");
                    if (task_queue.size() > 0) {
                        String awaiting_task = task_queue.poll();
                        workers_status.put(worker_id, awaiting_task);
                        try {
                            byte[] taskSerial = zk.getData("/dist24/tasks/" + awaiting_task, false, null);
                            assignTask(worker_id, awaiting_task, taskSerial);
                            watchOnAssignedWorkerTask(worker_id, awaiting_task);
                        } catch (NodeExistsException nee) {
                            System.out.println(nee);
                        } catch (KeeperException ke) {
                            System.out.println(ke);
                        } catch (InterruptedException ie) {
                            System.out.println(ie);
                        }
                    }
                    break;
                default:
                    // printBlue("Default");
                    // printBlue("path : " + path);
                    watchOnAssignedWorkerTask(worker_id, task_id);
                    break;
            }
        }
    };

    // Master deal with new workers created
    Watcher newWorkerWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist24/workers")) {
                printRed("[Master newWorkerWatcher]: master is notified by a new WORKER submitted");
                getNewWorker();
            }
        }
    };

    AsyncCallback.ChildrenCallback newWorkerCallBack = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            for (String c : children) {
                if (!workers_status.containsKey(c)) {
                    printYellow("---------------------- NEW WORKER [Master] ----------------------");
                    printRed("[Master newWorkerCallBack]: master finds a new WORKER: " + c);
                    workers_status.put(c, "idle");
                    printRed("[Master newWorkerCallBack]: master registers the new WORKER: " + c);
                } else {
                    printRed("[Master newTaskCallBack]: " + c + " is already in the master task list");
                }
            }
        }
    };

    // Create a new worker server
    void getNewWorker() {
        zk.getChildren("/dist24/workers", newWorkerWatcher, newWorkerCallBack, null);
    }

    void createWorker(String worker_name) {
        zk.create("/dist24/workers/worker_" + worker_name, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createWorkerCallback, null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case OK:
                    printRed("[" + name + "]: Registered successfully");
                    break;
                case NODEEXISTS:
                    printRed("[" + name + "]: Already Registered");
                    break;
                default:
                    printRed("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    // Worker get new task assignment
    void getWorkerAssignedTask(String worker_id) {
        zk.getChildren("/dist24/workers/worker_" + worker_id, newWorkerTaskWatcher, newWorkerTaskCallBack, null);
    }

    // Master deal with new workers created
    Watcher newWorkerTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged
                    && e.getPath().equals("/dist24/workers/worker_" + pinfo)) {
                getWorkerAssignedTask(pinfo);
            }
        }
    };

    AsyncCallback.ChildrenCallback newWorkerTaskCallBack = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            for (String c : children) {
                if (!visited_children.contains(c)) {
                    printYellow("---------------------- NEW TASK [WORKER] ----------------------");
                    printRed("[Worker newWorkerTaskCallBack]: worker is assigned a new TASK: " + c);
                    visited_children.add(c);
                    new Thread(new WorkerRunnable(pinfo, c)).start();
                } else {
                    printRed("[Worker newWorkerTaskCallBack]: " + c + " is already assigned");
                }
            }
        }
    };

    class WorkerRunnable implements Runnable {
        private String worker_id;
        private String task_id;

        public WorkerRunnable(String worker_id, String task_id) {
            this.worker_id = worker_id;
            this.task_id = task_id;
        }

        public void run() {
            workerCompute(this.worker_id, this.task_id);
        }

        private void workerCompute(String worker_id, String task_id) {
            try {
                printBlue("Worker executing TASK " + task_id + " on a new THREAD: " + Thread.currentThread().getId());
                byte[] taskSerial = zk.getData("/dist24/workers/worker_" + worker_id + "/" + task_id, false, null);
                ByteArrayInputStream bis_worker = new ByteArrayInputStream(taskSerial);
                ObjectInput in_worker = new ObjectInputStream(bis_worker);
                DistTask dt_worker = (DistTask) in_worker.readObject();
                dt_worker.compute();
                // printBlue("Computation finished. Pi = " + dt_worker.getPi());
                ByteArrayOutputStream bos_worker = new ByteArrayOutputStream();
                ObjectOutputStream oos_worker = new ObjectOutputStream(bos_worker);
                oos_worker.writeObject(dt_worker);
                oos_worker.flush();
                taskSerial = bos_worker.toByteArray();
                zk.create("/dist24/tasks/" + task_id + "/result", taskSerial, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                zk.delete("/dist24/workers/worker_" + worker_id + "/" + task_id, -1, null, null);
                printYellow("WORKER DONE WITH TASK " + task_id);
            } catch (NodeExistsException nee) {
                System.out.println(nee);
            } catch (KeeperException ke) {
                System.out.println(ke);
            } catch (InterruptedException ie) {
                System.out.println(ie);
            } catch (ClassNotFoundException ce) {
                System.out.println(ce);
            } catch (IOException io) {
                System.out.println(io);
            }
        }
    }

    // Assigning task to worker
    void assignTask(String worker_id, String task_id, byte[] taskSerial) {
        zk.create("/dist24/workers/" + worker_id + "/" + task_id, taskSerial, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, createWorkerTaskCallback, null);
    }

    AsyncCallback.StringCallback createWorkerTaskCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case OK:
                    printRed("[" + name + "]: Task assigned successfully");
                    break;
                case NODEEXISTS:
                    printRed("[" + name + "]: Task Already assigned");
                    break;
                default:
                    printRed("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    DistProcess(String zkhost) {
        zkServer = zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
        zk = new ZooKeeper(zkServer, 1000, newWorkerWatcher); // connect to ZK.
        try {
            runForMaster(); // See if you can become the master (i.e, no other master exists)
            isMaster = true;
            // master node listen on the /workers directory to see if any new worker is
            // added to the system
            getNewWorker();
            getNewlySubmittedTasks(); // Install monitoring on any new tasks that will be created.
            // TODO monitor for worker tasks?

        } catch (NodeExistsException nee) {
            // worker node is created
            isMaster = false;
            createWorker(pinfo);
            getWorkerAssignedTask(pinfo);
            // workers_status.put(pinfo, true);
            // System.out.println("HashMap is : " + workers_status);
            // zk.getData("/dist24/workers/worker_" + pinfo, this, this, null);
            // getWorkerTask();
        } // TODO: What else will you need if this was a worker process?

        // zk.create("/dist24/tasks/task-", dTaskSerial, Ids.OPEN_ACL_UNSAFE,
        // CreateMode.PERSISTENT_SEQUENTIAL);

        printBlue("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));
    }

    // Master fetching task znodes...
    // void getTasks() {
    // zk.getChildren("/dist24/tasks", this, this, null);
    // }

    // // Master fetching task znodes...
    // void getWorkers() {
    // zk.getChildren("/dist24/workers", this, this, null);
    // }

    // void getWorkerTask() {
    // zk.getChildren("/dist24/workers/worker_" + pinfo, this, this, null);
    // }

    // Try to become the master.
    void runForMaster() throws UnknownHostException, KeeperException, InterruptedException {
        // Try to create an ephemeral node to be the master, put the hostname and pid
        // of
        // this process as the data.
        // This is an example of Synchronous API invocation as the function waits for
        // the execution and no callback is involved..
        // zk.create("/dist24/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE,
        // CreateMode.EPHEMERAL);
        zk.create("/dist24/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e) {
        // Get watcher notifications.

        // !! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        // including in other functions called from here.
        // Your will be essentially holding up ZK client library
        // thread and you will not get other notifications.
        // Instead include another thread in your program logic that
        // does the time consuming "work" and notify that thread from here.

        // System.out.println("DISTAPP : Event received : " + e);

        // if (isMaster) {
        // if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged &&
        // e.getPath().equals("/dist24/tasks")) {
        // // There has been changes to the children of the node.
        // // We are going to re-install the Watch as well as request for the list of
        // // the
        // // children.
        // getTasks();
        // }
        // // Master should be notified if any new workers znodes are added to tasks.
        // else if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged
        // && e.getPath().equals("/dist24/workers")) {
        // // There has been changes to the children of the node.
        // // We are going to re-install the Watch as well as request for the list of
        // // the
        // // children.
        // getWorkers();
        // }

        // } else {
        // if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged
        // && e.getPath().equals("/dist24/workers/worker_" + pinfo)) {
        // // There has been changes to the children of the node.
        // // We are going to re-install the Watch as well as request for the list of
        // // the
        // // children.
        // getWorkerTask();
        // }
        // }
    }

    // Implementing the AsyncCallback.StatCallback interface. This will be
    // invoked
    // by the zk.exists
    public void processResult(int rc, String path, Object ctx, Stat stat) {

        // // !! IMPORTANT !!
        // // Do not perform any time consuming/waiting steps here
        // // including in other functions called from here.
        // // Your will be essentially holding up ZK client library
        // // thread and you will not get other notifications.
        // // Instead include another thread in your program logic that
        // // does the time consuming "work" and notify that thread from here.

        // // System.out.println("DISTAPP : processResult : StatCallback : " + rc + ":"
        // // +
        // // path + ":" + ctx + ":" + stat);
        // // System.out.println("Code.get(rc): " + Code.get(rc));
        // switch (Code.get(rc)) {
        // case OK:
        // // The result znode is ready.
        // // System.out.println("DISTAPP : processResult : StatCallback : OK");
        // // Ask for data in the result znode (asynchronously). We do not have to watch
        // // this znode anymore.
        // // System.out.println("OK OK OK OK");
        // zk.exists(path, this, this, null);
        // break;
        // case NONODE:
        // // The result znode was not ready, we will just make sure to reinstall the
        // // watcher.
        // // Ideally we should come here only once!, if at all. That will be the time
        // // we
        // // called
        // // exists on the result znode immediately after creating the task znode.
        // printYellow("DISTAPP : processResult : StatCallback : " + Code.get(rc));
        // // zk.exists(taskNodeName + "/result", this, null, null);
        // System.out.println("Path: " + path);
        // System.out.println("NO NODE NO NODE");
        // String[] tokens = path.split("/");
        // // for (int i = 0; i < tokens.length; i++) {
        // // System.out.println(i);
        // // System.out.println(tokens[i]);
        // // }
        // String worker_id = tokens[3];

        // workers_status.put(worker_id, "idle");
        // printBlue(workers_status.toString());
        // printBlue(worker_id + " is back to idle");

        // if (!task_queue.isEmpty()) {
        // String task_id = task_queue.poll();
        // printBlue("Assigning queuing task: " + task_id + " to worker " + worker_id);
        // workers_status.put(worker_id, task_id);
        // // get the task object
        // try {
        // byte[] taskSerial = zk.getData("/dist24/tasks/" + task_id, false, null);
        // printBlue("after getting available worker node: " + worker_id);
        // // create a task object under the idle worker
        // zk.create("/dist24/workers/" + worker_id + "/" + task_id, taskSerial,
        // Ids.OPEN_ACL_UNSAFE,
        // CreateMode.PERSISTENT);
        // zk.exists("/dist24/workers/" + worker_id + "/" + task_id, this, this, null);
        // visited_children.add(task_id);
        // } catch (NodeExistsException nee) {
        // System.out.println(nee);
        // } catch (KeeperException ke) {
        // System.out.println(ke);
        // } catch (InterruptedException ie) {
        // System.out.println(ie);
        // }
        // }
        // printYellow("NO NODE HANDLED");
        // break;
        // default:
        // System.out.println("DISTAPP : processResult : StatCallback : " +
        // Code.get(rc));
        // break;
        // }

    }

    // Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children) {

        // !! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        // including in other functions called from here.
        // Your will be essentially holding up ZK client library
        // thread and you will not get other notifications.
        // Instead include another thread in your program logic that
        // does the time consuming "work" and notify that thread from here.

        // This logic is for master !!
        // Every time a new task znode is created by the client, this will be
        // invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        // Also have a mechanism to assign these tasks to a "Worker" process.
        // The worker must invoke the "compute" function of the Task send by the
        // client.
        // What to do if you do not have a free worker process?
        // System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" +
        // ctx);
        // System.out.println("LOG : callback called by : " + pinfo + "; " + "Path: "
        // +
        // path);
        // for (String c : children) {
        // if (visited_children.contains(c)) {
        // printRed("Children " + c + " has been handled before.");
        // continue;
        // }
        // String whoami = (isMaster) ? "Master" : ("worker_" + pinfo);

        // printYellow(whoami + " is awared of changes in: " + path + "; " + c);
        // try {
        // // TODO There is quite a bit of worker specific activities here,
        // // that should be moved done by a process function as the worker.
        // // TODO!! This is not a good approach, you should get the data using an async
        // // version of the API.
        // // /dist24/workers/worker-111@lab2-11/task-finish

        // if ("/dist24/workers".equals(path)) { // If a new worker node is created
        // // (callback by master)
        // printYellow("---------------------- NEW WORKER ----------------------");
        // System.out.println("New worker: " + c);
        // // add the worker to the master's record
        // workers_status.put(c, "idle");
        // visited_children.add(c);
        // printYellow("NEW WORKER -- DONE");
        // } else if ("/dist24/tasks".equals(path)) { // If a task node is submitted
        // // (callback by master)
        // printYellow("---------------------- NEW TASK ----------------------");
        // // find the idle worker
        // String worker_id = getIdleWorker(workers_status);

        // if (worker_id.equals("")) { // no available worker right now
        // task_queue.add(c); // add the task into the task queue
        // printBlue("Adding " + c + " into the task queue");
        // printYellow("NEW TASK PUT IN QUEUE");
        // // thread :

        // } else {
        // workers_status.put(worker_id, c);
        // // get the task object
        // byte[] taskSerial = zk.getData("/dist24/tasks/" + c, false, null);
        // printBlue("Found an idle worker : " + worker_id);
        // // create a task object under the idle worker
        // System.out.println("Assigning " + c + " to worker " + worker_id);
        // zk.create("/dist24/workers/" + worker_id + "/" + c, taskSerial,
        // Ids.OPEN_ACL_UNSAFE,
        // CreateMode.PERSISTENT);
        // zk.exists("/dist24/workers/" + worker_id + "/" + c, this, this, null);
        // visited_children.add(c);
        // printYellow("NEW TASK DONE");
        // }
        // // /dist24/workers/worker-111@lab2-11
        // } else if (("/dist24/workers/worker_" + pinfo).equals(path)) { // If a task
        // // node assigned to a worker
        // // (callback by worker)
        // printYellow("---------------------- WORKER GETS NEW
        // TASK----------------------");
        // printBlue("LOG : Worker: " + pinfo + " gets a new task " + c);
        // byte[] taskSerial = zk.getData("/dist24/workers/worker_" + pinfo + "/" + c,
        // false, null);
        // ByteArrayInputStream bis_worker = new ByteArrayInputStream(taskSerial);
        // ObjectInput in_worker = new ObjectInputStream(bis_worker);
        // DistTask dt_worker = (DistTask) in_worker.readObject();
        // dt_worker.compute();
        // ByteArrayOutputStream bos_worker = new ByteArrayOutputStream();
        // ObjectOutputStream oos_worker = new ObjectOutputStream(bos_worker);
        // oos_worker.writeObject(dt_worker);
        // oos_worker.flush();
        // taskSerial = bos_worker.toByteArray();
        // zk.create("/dist24/tasks/" + c + "/result", taskSerial, Ids.OPEN_ACL_UNSAFE,
        // CreateMode.PERSISTENT);
        // // clean up task node under the worker

        // zk.delete("/dist24/workers/worker_" + pinfo + "/" + c, -1, null, this);
        // printYellow("WORKER DONE WITH TASK");
        // } else {
        // printYellow("DISTAPP : processResult : getChildrenCallback : " +
        // Code.get(rc));
        // }
        // } catch (NodeExistsException nee) {
        // System.out.println(nee);
        // } catch (KeeperException ke) {
        // System.out.println(ke);
        // } catch (InterruptedException ie) {
        // System.out.println(ie);
        // } catch (ClassNotFoundException ce) {
        // System.out.println(ce);
        // } catch (IOException io) {
        // System.out.println(io);
        // }
        // }
    }

    public static void main(String args[]) throws Exception {
        // Create a new process
        // Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        // Replace this with an approach that will make sure that the process is up and
        // running forever.
        while (true) {

        }
        // Thread.sleep(10000);
    }
}
