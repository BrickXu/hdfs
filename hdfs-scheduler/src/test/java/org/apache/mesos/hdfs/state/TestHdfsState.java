package org.apache.mesos.hdfs.state;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.protobuf.DiskInfoBuilder;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hdfs.TestSchedulerModule;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.TaskStatusFactory;
import org.apache.mesos.protobuf.CommandInfoBuilder;
import org.apache.mesos.protobuf.ExecutorInfoBuilder;
import org.apache.mesos.protobuf.OfferBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.apache.mesos.protobuf.TaskStatusBuilder;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class TestHdfsState {
  private final Injector injector = Guice.createInjector(new TestSchedulerModule());
  private SecureRandom random = new SecureRandom();
  private static final String testIdName = "framework";
  private static final String TEST_HOST = "host";
  private static final String TEST_TYPE = "type";
  private static final String TEST_NAME = "name";

  @Test
  public void testTerminalStatusUpdate()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createTask();
    state.recordTask(inTask);

    TaskStatus status = createTaskStatus(inTask.getId().getValue(), TaskState.TASK_FAILED);
    state.update(null, status);
    List<TaskRecord> tasks = state.getTasks();
    assertEquals(0, tasks.size());
  }

  @Test
  public void testStoreVolumeRecord()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    VolumeRecord inVolume = createVolume("store-test-persistence-id", "store-test-task-id");
    state.recordVolume(inVolume);

    List<VolumeRecord> volumes = state.getVolumes();
    assertEquals(1, volumes.size());
  }

  @Test
  public void testFindOrphanedVolume()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    // Store a TaskRecord
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createTask();
    state.recordTask(inTask);

    // Store a Volume NOT associated with that Task
    VolumeRecord inVolume = createVolume("orphan-test-persistence-id", "bad-task-id");
    state.recordVolume(inVolume);

    // Verify that we find the expected orphaned volume
    List<VolumeRecord> orphanedVolumes = state.getOrphanedVolumes();
    assertEquals(1, orphanedVolumes.size());

    VolumeRecord orphanedVolume = orphanedVolumes.get(0);
    assertEquals(inVolume.getInfo(), orphanedVolume.getInfo());
    assertEquals(inVolume.getTaskId(), orphanedVolume.getTaskId());
  }

  @Test
  public void testNotFindOrphanedVolume()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    // Store a TaskRecord
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createTask();
    state.recordTask(inTask);

    // Store a Volume associated with that Task
    VolumeRecord inVolume = createVolume("orphan-test-persistence-id", inTask.getId().getValue());
    state.recordVolume(inVolume);

    // Verify that we fail to find an orphaned volume
    List<VolumeRecord> orphanedVolumes = state.getOrphanedVolumes();
    assertEquals(0, orphanedVolumes.size());
  }

  @Test
  public void testNonTerminalStatusUpdate()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createTask();
    state.recordTask(inTask);

    TaskStatus status = createTaskStatus(inTask.getId().getValue(), TaskState.TASK_RUNNING);
    state.update(null, status);
    List<TaskRecord> tasks = state.getTasks();
    assertEquals(1, tasks.size());

    TaskRecord outTask = tasks.get(0);
    assertEquals(status, outTask.getStatus());
  }

  @Test
  public void testHostOccupied()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = createDefaultState();
    assertFalse(state.hostOccupied("wrong_host", TEST_TYPE));
    assertFalse(state.hostOccupied(TEST_HOST, "wrong_type"));
    assertFalse(state.hostOccupied("wrong_host", "wrong_type"));
    assertTrue(state.hostOccupied(TEST_HOST, TEST_TYPE));
  }

  @Test
  public void testGetNameNodeTasks()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createNameNodeTask();
    state.recordTask(inTask);

    List<TaskRecord> nameTasks = state.getNameNodeTasks();
    assertEquals(1, nameTasks.size());

    List<TaskRecord> journalTasks = state.getJournalNodeTasks();
    assertEquals(0, journalTasks.size());
  }

  @Test
  public void testGetJournalNodeTasks()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createJournalNodeTask();
    state.recordTask(inTask);

    List<TaskRecord> journalTasks = state.getJournalNodeTasks();
    assertEquals(1, journalTasks.size());

    List<TaskRecord> nameTasks = state.getNameNodeTasks();
    assertEquals(0, nameTasks.size());
  }

  @Test
  public void testNameNodesInitialized()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    assertFalse(state.nameNodesInitialized());

    TaskRecord namenode1Task = createNameNodeTask();
    TaskRecord namenode2Task = createNameNodeTask();
    state.recordTask(namenode1Task);
    state.recordTask(namenode2Task);

    TaskStatus status1 = TaskStatusFactory.createNameNodeStatus(namenode1Task.getId(), true);
    TaskStatus status2 = TaskStatusFactory.createNameNodeStatus(namenode2Task.getId(), true);

    state.update(null, status1);
    assertFalse(state.nameNodesInitialized());

    state.update(null, status2);
    assertTrue(state.nameNodesInitialized());
  }

  private HdfsState createDefaultState()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    TaskRecord inTask = createTask();
    state.recordTask(inTask);
    return state;
  }

  private TaskRecord createTask() {
    return createTask(TEST_NAME);
  }

  private TaskRecord createNameNodeTask() {
    return createTask(HDFSConstants.NAME_NODE_ID);
  }

  private TaskRecord createJournalNodeTask() {
    return createTask(HDFSConstants.JOURNAL_NODE_ID);
  }

  private TaskRecord createTask(String name) {
    List<Resource> resources = createResourceList();
    ExecutorInfo execInfo = createExecutorInfo();
    Offer offer = createOffer();
    String taskIdName = createTaskIdName();
    return new TaskRecord(resources, execInfo, offer, name, TEST_TYPE, taskIdName);
  }

  private VolumeRecord createVolume(String persistenceId, String taskId) {
    DiskInfoBuilder builder = new DiskInfoBuilder();
    builder.setPersistence(persistenceId);
    DiskInfo info = builder.build();
    TaskID id = TaskID.newBuilder().setValue(taskId).build();

    return new VolumeRecord(info, id);
  }

  public String createTaskIdName() {
    return "taskIdName_" + new BigInteger(130, random).toString(32);
  }

  private List<Resource> createResourceList() {
    Resource r = ResourceBuilder.createScalarResource("name", 1, "role");
    List<Resource> resources = new ArrayList<Resource>();
    resources.add(r);
    return resources;
  }

  private TaskStatus createTaskStatus(String taskId, TaskState state) {
    return TaskStatusBuilder.createTaskStatus(taskId, "slave", state, "From Test");
  }


  private ExecutorInfo createExecutorInfo() {

    ExecutorInfoBuilder builder = new ExecutorInfoBuilder("executor", "executor");
    builder.addCommandInfo(new CommandInfoBuilder()
      .addUri("http://test_url/")
      .build());
    return builder.build();
  }

  private Offer createOffer() {
    return new OfferBuilder("offer1", "framework", "slave", TEST_HOST).build();
  }
}
