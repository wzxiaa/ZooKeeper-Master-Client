import java.io.*;

import java.util.*;

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
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.StatCallback { // ,
                                                                                                          // AsyncCallback.StatCallback
    public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_RED = "\u001B[31m";
	public static final String ANSI_YELLOW = "\u001B[33m";
	public static final String ANSI_BLUE = "\u001B[34m";
	ZooKeeper zk;
    String zkServer, pinfo;
    boolean isMaster = false;
    boolean idle = false;

    Queue<String> task_queue = new LinkedList<String>();
    HashMap<String, Boolean> workers_status = new HashMap<String, Boolean>();
	HashSet<String> visited_children = new HashSet<String>();

    DistProcess(String zkhost) {
        zkServer = zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
        zk = new ZooKeeper(zkServer, 1000, this); // connect to ZK.
        try {
            runForMaster(); // See if you can become the master (i.e, no other master exists)
            isMaster = true;
            // master node listen on the /workers directory to see if any new worker is
            // added to the system
            getWorkers();
            getTasks(); // Install monitoring on any new tasks that will be created.
                        // TODO monitor for worker tasks?

        } catch (NodeExistsException nee) {
            // worker node is created
            isMaster = false;
            zk.create("/dist24/workers/worker_" + pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Creating worker node  : " + "worker_" + pinfo);
            // workers_status.put(pinfo, true);
            // System.out.println("HashMap is : " + workers_status);
            // zk.getData("/dist24/workers/worker_" + pinfo, this, this, null);
            getWorkerTask();
        } // TODO: What else will you need if this was a worker process?

        // zk.create("/dist24/tasks/task-", dTaskSerial, Ids.OPEN_ACL_UNSAFE,
        // CreateMode.PERSISTENT_SEQUENTIAL);

        System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));
    }

    // Master fetching task znodes...
    void getTasks() {
        zk.getChildren("/dist24/tasks", this, this, null);
    }

    // Master fetching task znodes...
    void getWorkers() {
        zk.getChildren("/dist24/workers", this, this, null);
    }

    void getWorkerTask() {
        zk.getChildren("/dist24/workers/worker_" + pinfo, this, this, null);
    }

    // Try to become the master.
    void runForMaster() throws UnknownHostException, KeeperException, InterruptedException {
        // Try to create an ephemeral node to be the master, put the hostname and pid of
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

        System.out.println("DISTAPP : Event received : " + e);
        // Master should be notified if any new znodes are added to tasks.
        if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist24/tasks")) {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the
            // children.
            getTasks();
        }
        // Master should be notified if any new workers znodes are added to tasks.
        else if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist24/workers")) {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the
            // children.
            getWorkers();
        }
        // Worker should be notified if any new workers task znodes are added to tasks.
        else if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged
                && e.getPath().equals("/dist24/workers/worker_" + pinfo)) {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the
            // children.
            getWorkerTask();
        }

        // else if (e.getType() == Watcher.Event.EventType.NodeDeleted
        // && e.getPath().equals("/dist24/workers/worker_" + pinfo)) {
        // // There has been changes to the children of the node.
        // // We are going to re-install the Watch as well as request for the list of
        // the
        // // children.
        // System.out.println("Detected Node Deleted!!!");
        // }
    }

    // Implementing the AsyncCallback.StatCallback interface. This will be invoked
    // by the zk.exists
    public void processResult(int rc, String path, Object ctx, Stat stat) {

        // !! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        // including in other functions called from here.
        // Your will be essentially holding up ZK client library
        // thread and you will not get other notifications.
        // Instead include another thread in your program logic that
        // does the time consuming "work" and notify that thread from here.

        // System.out.println("DISTAPP : processResult : StatCallback : " + rc + ":" +
        // path + ":" + ctx + ":" + stat);
        // System.out.println("Code.get(rc): " + Code.get(rc));
        switch (Code.get(rc)) {
            case OK:
                // The result znode is ready.
                // System.out.println("DISTAPP : processResult : StatCallback : OK");
                // Ask for data in the result znode (asynchronously). We do not have to watch
                // this znode anymore.
                // System.out.println("OK OK OK OK");
                zk.exists(path, this, this, null);
                break;
            case NONODE:
                // The result znode was not ready, we will just make sure to reinstall the
                // watcher.
                // Ideally we should come here only once!, if at all. That will be the time
                // we
                // called
                // exists on the result znode immediately after creating the task znode.
                printYellow("DISTAPP : processResult : StatCallback : " + Code.get(rc));
                // zk.exists(taskNodeName + "/result", this, null, null);
                System.out.println("Path: " + path);
                System.out.println("NO NODE NO NODE");
                String[] tokens = path.split("/");
                // for (int i = 0; i < tokens.length; i++) {
                // System.out.println(i);
                // System.out.println(tokens[i]);
                // }
                String worker_id = tokens[3];

                workers_status.put(worker_id, true);
                printBlue(workers_status.toString());
                printBlue(worker_id + " is back to idle");

                if (!task_queue.isEmpty()) {
                    String task_id = task_queue.poll();
                    printBlue("Assigning queuing task: " + task_id + " to worker " + worker_id);
                    workers_status.put(worker_id, false);
                    // get the task object
                    try {
                        byte[] taskSerial = zk.getData("/dist24/tasks/" + task_id, false, null);
                        printBlue("after getting available worker node: " + worker_id);
                        // create a task object under the idle worker
                        zk.create("/dist24/workers/" + worker_id + "/" + task_id, taskSerial, Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        zk.exists("/dist24/workers/" + worker_id + "/" + task_id, this, this, null);
						visited_children.add(task_id);
                    } catch (NodeExistsException nee) {
                        System.out.println(nee);
                    } catch (KeeperException ke) {
                        System.out.println(ke);
                    } catch (InterruptedException ie) {
                        System.out.println(ie);
                    }
                }
				printYellow("NO NODE HANDLED");
                break;
            default:
                System.out.println("DISTAPP : processResult : StatCallback : " + Code.get(rc));
                break;
        }

    }

    public String getIdleWorker(HashMap<String, Boolean> map) {
        String id = "";
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            if ((Boolean) pair.getValue()) {
                id = (String) pair.getKey();
                return id;
            }
        }
        printRed("no worker available");
        return id;
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
        // Every time a new task znode is created by the client, this will be invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        // Also have a mechanism to assign these tasks to a "Worker" process.
        // The worker must invoke the "compute" function of the Task send by the client.
        // What to do if you do not have a free worker process?
        // System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" +
        // ctx);
        // System.out.println("LOG : callback called by : " + pinfo + "; " + "Path: " +
        // path);
        for (String c : children) {
			if(visited_children.contains(c)){
				printRed("Children " + c + " has been handled before.");
				continue;
			}
            String whoami = (isMaster) ? "Master" : ("worker_" + pinfo);

            printYellow(whoami + " is awared of changes in: " + path + "; " + c);
            try {
                // TODO There is quite a bit of worker specific activities here,
                // that should be moved done by a process function as the worker.
                // TODO!! This is not a good approach, you should get the data using an async
                // version of the API.
                // /dist24/workers/worker-111@lab2-11/task-finish

                if ("/dist24/workers".equals(path)) { // If a new worker node is created (callback by master)
                    printYellow("---------------------- NEW WORKER ----------------------");
					System.out.println("New worker: " + c);
                    // add the worker to the master's record
                    workers_status.put(c, true);
					visited_children.add(c);
					printYellow("NEW WORKER -- DONE");
                } else if ("/dist24/tasks".equals(path)) { // If a task node is submitted (callback by master)
                    printYellow("---------------------- NEW TASK ----------------------");
                    // find the idle worker
                    String worker_id = getIdleWorker(workers_status);

                    if (worker_id.equals("")) { // no available worker right now
                        task_queue.add(c); // add the task into the task queue
                        printBlue("Adding " + c + " into the task queue");
						printYellow("NEW TASK PUT IN QUEUE");
                        // thread :

                    } else {
                        workers_status.put(worker_id, false);
                        // get the task object
                        byte[] taskSerial = zk.getData("/dist24/tasks/" + c, false, null);
                        printBlue("Found an idle worker : " + worker_id);
                        // create a task object under the idle worker
                        System.out.println("Assigning " + c + " to worker " + worker_id);
                        zk.create("/dist24/workers/" + worker_id + "/" + c, taskSerial, Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        zk.exists("/dist24/workers/" + worker_id + "/" + c, this, this, null);
						visited_children.add(c);
						printYellow("NEW TASK DONE");
                    }
                    // /dist24/workers/worker-111@lab2-11
                } else if (("/dist24/workers/worker_" + pinfo).equals(path)) { // If a task node assigned to a worker
                                                                               // (callback by worker)
                    printYellow("---------------------- WORKER GETS NEW TASK ----------------------");
                    printBlue("LOG : Worker: " + pinfo + " gets a new task " + c);
                    byte[] taskSerial = zk.getData("/dist24/workers/worker_" + pinfo + "/" + c, false, null);
                    ByteArrayInputStream bis_worker = new ByteArrayInputStream(taskSerial);
                    ObjectInput in_worker = new ObjectInputStream(bis_worker);
                    DistTask dt_worker = (DistTask) in_worker.readObject();
                    dt_worker.compute();
                    ByteArrayOutputStream bos_worker = new ByteArrayOutputStream();
                    ObjectOutputStream oos_worker = new ObjectOutputStream(bos_worker);
                    oos_worker.writeObject(dt_worker);
                    oos_worker.flush();
                    taskSerial = bos_worker.toByteArray();
                    zk.create("/dist24/tasks/" + c + "/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    // clean up task node under the worker

                    zk.delete("/dist24/workers/worker_" + pinfo + "/" + c, -1, null, this);
					printYellow("WORKER DONE WITH TASK");
                } else {
                    printYellow("DISTAPP : processResult : getChildrenCallback : " + Code.get(rc));
				}
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

	public void printBlue(String str){
		System.out.println(ANSI_BLUE + str + ANSI_RESET);
	}

	public void printYellow(String str){
		System.out.println(ANSI_YELLOW + str + ANSI_RESET);
	}

	public void printRed(String str){
		System.out.println(ANSI_RED + str + ANSI_RESET);
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
