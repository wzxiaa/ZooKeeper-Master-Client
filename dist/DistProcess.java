import java.io.*;

import java.util.*;
import java.util.concurrent.*;
// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;
import java.nio.charset.StandardCharsets;

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
                            watchOnAssignedWorkerTask(worker_id);
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
    void watchOnAssignedWorkerTask(String worker_id) {
        zk.getData("/dist24/workers/" + worker_id, null, workerTaskFinishedCallBack, null);
    }

    AsyncCallback.DataCallback workerTaskFinishedCallBack = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            String[] tokens = path.split("/");
            String worker_id = tokens[3];
            // String task_id = tokens[4];
            String taskAssigned = new String(data, StandardCharsets.UTF_8);
            if (taskAssigned.equals("idle")) {
                printBlue("[Matser workerTaskFinishedCallBack]: Master detects " + worker_id + " finished a task");
                workers_status.put(worker_id, "idle");
                if (task_queue.size() > 0) {
                    String awaiting_task = task_queue.poll();
                    workers_status.put(worker_id, awaiting_task);
                    try {
                        byte[] taskSerial = zk.getData("/dist24/tasks/" + awaiting_task, false, null);
                        assignTask(worker_id, awaiting_task, taskSerial);
                        watchOnAssignedWorkerTask(worker_id);
                    } catch (NodeExistsException nee) {
                        System.out.println(nee);
                    } catch (KeeperException ke) {
                        System.out.println(ke);
                    } catch (InterruptedException ie) {
                        System.out.println(ie);
                    }
                }
            } else {
                watchOnAssignedWorkerTask(worker_id);
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
                    if (task_queue.size() > 0) {
                        String awaiting_task = task_queue.poll();
                        workers_status.put(c, awaiting_task);
                        try {
                            byte[] taskSerial = zk.getData("/dist24/tasks/" + awaiting_task, false, null);
                            assignTask(c, awaiting_task, taskSerial);
                            watchOnAssignedWorkerTask(c);
                        } catch (NodeExistsException nee) {
                            System.out.println(nee);
                        } catch (KeeperException ke) {
                            System.out.println(ke);
                        } catch (InterruptedException ie) {
                            System.out.println(ie);
                        }
                    }

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
        // String status = "idle";
        // Byte[] status_byte = status.getBytes();
        zk.create("/dist24/workers/worker_" + worker_name, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
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
        zk.getData("/dist24/workers/worker_" + worker_id, newWorkerTaskWatcher, newWorkerTaskCallBack, null);
    }

    // // Master deal with new workers created
    Watcher newWorkerTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Watcher.Event.EventType.NodeDataChanged
                    && e.getPath().equals("/dist24/workers/worker_" + pinfo)) {
                getWorkerAssignedTask(pinfo);
            }
        }
    };

    AsyncCallback.DataCallback newWorkerTaskCallBack = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (data != null && data.length > 0) {
                String task_id = new String(data, StandardCharsets.UTF_8);
                if (!task_id.equals("idle")) {
                    printYellow("---------------------- NEW TASK [WORKER] ----------------------");

                    printRed("[Worker newWorkerTaskCallBack]: worker is assigned a new TASK: " + task_id);
                    // visited_children.add(c);
                    new Thread(new WorkerRunnable(pinfo, task_id)).start();
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
                byte[] taskSerial = zk.getData("/dist24/tasks/" + task_id, false, null);
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
                zk.setData("/dist24/workers/worker_" + worker_id, "idle".getBytes(), -1);
                // printYellow("WORKER DONE WITH TASK " + task_id);
                printYellow("WORKER DONE WITH TASK ");
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
        zk.setData("/dist24/workers/" + worker_id, task_id.getBytes(), -1,
                new createWorkerTaskCallback(task_id, worker_id), null);
    }

    class createWorkerTaskCallback implements AsyncCallback.StatCallback {
        private String task_id;
        private String worker_id;

        public createWorkerTaskCallback(String task_id, String worker_id) {
            this.task_id = task_id;
            this.worker_id = worker_id;
        }

        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case OK:
                    printRed(
                            "[Master createWorkerTaskCallback]: " + task_id + " assigned successfully to " + worker_id);
                    break;
                case NODEEXISTS:
                    printRed("[Master createWorkerTaskCallback]: Task Already assigned");
                    break;
                default:
                    printRed("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    }

    // AsyncCallback.StatCallback createWorkerTaskCallback = new
    // AsyncCallback.StatCallback() {
    // public void processResult(int rc, String path, Object ctx, Stat stat) {
    // switch (Code.get(rc)) {
    // case OK:
    // printRed("[" + stat + "]: Task assigned successfully");
    // break;
    // case NODEEXISTS:
    // printRed("[" + stat + "]: Task Already assigned");
    // break;
    // default:
    // printRed("Something went wrong: " + KeeperException.create(Code.get(rc),
    // path));
    // }
    // }
    // };

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
        }

        printBlue("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));
    }

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
    }

    // Implementing the AsyncCallback.StatCallback interface. This will be
    // invoked
    // by the zk.exists
    public void processResult(int rc, String path, Object ctx, Stat stat) {

    }

    // Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children) {
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
