package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.List;
import java.util.ArrayList;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.VolumeRecord;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.TestSchedulerModule;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.apache.mesos.protobuf.OfferBuilder;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Resource.DiskInfo.Persistence;
import org.apache.mesos.Protos.TaskID;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

public class TestNodeConstraints {
  private final Injector injector = Guice.createInjector(new TestSchedulerModule());
  private HdfsFrameworkConfig config = injector.getInstance(HdfsFrameworkConfig.class);
  private NodeConfig journalConfig = config.getNodeConfig(HDFSConstants.JOURNAL_NODE_ID);
  private ResourceBuilder resourceBuilder = new ResourceBuilder(config.getRole());

  private final int TARGET_JOURNAL_COUNT = config.getJournalNodeCount();
  private final int ENOUGH_JOURNAL_MEM = (int) ConstraintUtils.getNeededMem(journalConfig.getMaxHeap(), config);
  private final double ENOUGH_JOURNAL_CPU = ConstraintUtils.getNeededCpus(journalConfig.getCpus(), config);
  private final int ENOUGH_JOURNAL_DISK = journalConfig.getDiskSize();
  private final VolumeRecord expectedVolume = createVolumeRecord("persistence-id", "task-id");

  @Mock
  HdfsState state;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void createJournalConstraint() throws Exception {
    ConstraintProvider provider = new ConstraintProvider(state, config, AcquisitionPhase.JOURNAL_NODES, expectedVolume);
    Constraint constraint = provider.getNextConstraint();
    assertTrue(constraint instanceof JournalConstraint);
  }

  @Test
  public void testCanSatisfyJournalConstraint() throws Exception {
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT-1);
    ConstraintProvider provider = new ConstraintProvider(state, config, AcquisitionPhase.JOURNAL_NODES, expectedVolume);
    Constraint constraint = provider.getNextConstraint();

    // An offer with enough resources should be accepted
    Offer offer = createOfferBuilder(ENOUGH_JOURNAL_CPU, ENOUGH_JOURNAL_MEM, ENOUGH_JOURNAL_DISK).build();
    assertTrue(constraint.canBeSatisfied(offer));

    // Offers which lack the required resources of each type should be rejected
    offer = createOfferBuilder(ENOUGH_JOURNAL_CPU-0.1, ENOUGH_JOURNAL_MEM, ENOUGH_JOURNAL_DISK).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createOfferBuilder(ENOUGH_JOURNAL_CPU, ENOUGH_JOURNAL_MEM-1, ENOUGH_JOURNAL_DISK).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createOfferBuilder(ENOUGH_JOURNAL_CPU, ENOUGH_JOURNAL_MEM, ENOUGH_JOURNAL_DISK-1).build();
    assertFalse(constraint.canBeSatisfied(offer));
  }

  @Test
  public void testSatisfiesJournalResourceConstraints() throws Exception {
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT-1);
    ConstraintProvider provider = new ConstraintProvider(state, config, AcquisitionPhase.JOURNAL_NODES, expectedVolume);
    Constraint constraint = provider.getNextConstraint();

    // An offer with enough resources which are not reserved should be rejected
    Offer offer = createOfferBuilder(ENOUGH_JOURNAL_CPU, ENOUGH_JOURNAL_MEM, ENOUGH_JOURNAL_DISK).build();
    assertFalse(constraint.isSatisfiedForReservations(offer));

    // An offer with enough reserved resources should be accepted
    offer = createReservedOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertTrue(constraint.isSatisfiedForReservations(offer));

    // An offer with enough reserved resources and a volume with the wrong persistence ID should be rejected 
    offer = createVolumeOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        "bad-persistence-id",
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.isSatisfiedForReservations(offer));
  }

  @Test
  public void testSatisfiesJournalVolumeConstraints() throws Exception {
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT-1);
    ConstraintProvider provider = new ConstraintProvider(state, config, AcquisitionPhase.JOURNAL_NODES, expectedVolume);
    Constraint constraint = provider.getNextConstraint();

    // An offer with enough reserved resouces, but no volumes should be rejected
    Offer offer = createReservedOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.isSatisfiedForVolumes(offer));

    // An offer with enough reserved resources and the correct persistence ID should be accepted
    offer = createVolumeOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        expectedVolume.getPersistenceId(),
        config.getRole(),
        config.getPrincipal()).build();
    assertTrue(constraint.isSatisfiedForVolumes(offer));

    // An offer with enough reserved resources and a volume with the wrong persistence ID should be rejected 
    offer = createVolumeOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        "bad-persistence-id",
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.isSatisfiedForVolumes(offer));
  }

  private OfferBuilder createOfferBuilder(double cpus, int mem, int diskSize) {
    return new OfferBuilder("offer-id", "framework-id", "slave-id", "hostname")
      .addResource(resourceBuilder.createCpuResource(cpus))
      .addResource(resourceBuilder.createMemResource(mem))
      .addResource(resourceBuilder.createDiskResource(diskSize));
  }

  private OfferBuilder createReservedOfferBuilder(double cpus, int mem, int diskSize, String role, String principal) {
    return new OfferBuilder("offer-id", "framework-id", "slave-id", "hostname")
      .addResource(resourceBuilder.reservedCpus(cpus, role, principal))
      .addResource(resourceBuilder.reservedMem(mem, role, principal))
      .addResource(resourceBuilder.reservedDisk(diskSize, role, principal));
  }

  private OfferBuilder createVolumeOfferBuilder(
      double cpus,
      int mem,
      int diskSize,
      String persistenceId,
      String role,
      String principal) {

    DiskInfo diskInfo = createDiskInfo(persistenceId);
    Resource diskWithVolume = resourceBuilder.reservedDisk(diskSize, role, principal);
    diskWithVolume = Resource.newBuilder(diskWithVolume)
      .setDisk(diskInfo).build();

    return new OfferBuilder("offer-id", "framework-id", "slave-id", "hostname")
      .addResource(resourceBuilder.reservedCpus(cpus, role, principal))
      .addResource(resourceBuilder.reservedMem(mem, role, principal))
      .addResource(diskWithVolume);

  }

  private VolumeRecord createVolumeRecord(String persistenceId, String taskId) {
    DiskInfo info = createDiskInfo(persistenceId);
    TaskID id = TaskID.newBuilder().setValue(taskId).build();

    return new VolumeRecord(info, id);
  }

  private DiskInfo createDiskInfo(String persistenceId) {
    return DiskInfo.newBuilder()
      .setPersistence(Persistence.newBuilder()
          .setId(persistenceId)).build();
  }
}
