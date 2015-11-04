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
import org.apache.mesos.hdfs.util.DnsResolver;
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

public class TestNodeOfferRequirements {
  private final Injector injector = Guice.createInjector(new TestSchedulerModule());
  private HdfsFrameworkConfig config = injector.getInstance(HdfsFrameworkConfig.class);
  private ResourceBuilder resourceBuilder = new ResourceBuilder(config.getRole());

  // JournalNode Config/Defaults
  private NodeConfig journalConfig = config.getNodeConfig(HDFSConstants.JOURNAL_NODE_ID);
  private final int TARGET_JOURNAL_COUNT = config.getJournalNodeCount();
  private final int ENOUGH_JOURNAL_MEM = (int) OfferRequirementUtils.getNeededMem(journalConfig.getMaxHeap(), config);
  private final int ENOUGH_JOURNAL_DISK = journalConfig.getDiskSize();
  private final double ENOUGH_JOURNAL_CPU = OfferRequirementUtils.getNeededCpus(journalConfig.getCpus(), config);

  // NameNode Config/Defaults
  private NodeConfig nameConfig = config.getNodeConfig(HDFSConstants.NAME_NODE_ID);
  private NodeConfig zkfcConfig = config.getNodeConfig(HDFSConstants.ZKFC_NODE_ID);
  private final int TARGET_NAME_COUNT = config.getNameNodeCount();
  private final int ENOUGH_NAME_MEM =
    (int) OfferRequirementUtils.getNeededMem(nameConfig.getMaxHeap() + zkfcConfig.getMaxHeap(), config);
  private final int ENOUGH_NAME_DISK = nameConfig.getDiskSize() + zkfcConfig.getDiskSize();
  private final double ENOUGH_NAME_CPU =
    OfferRequirementUtils.getNeededCpus(nameConfig.getCpus() + zkfcConfig.getCpus(), config);

  private final VolumeRecord expectedVolume = createVolumeRecord("persistence-id", "task-id");

  @Mock
  HdfsState state;

  @Mock
  DnsResolver dnsResolver;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void createJournalOfferRequirement() throws Exception {
    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.JOURNAL_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();
    assertTrue(constraint instanceof JournalOfferRequirement);
  }

  @Test
  public void testCanSatisfyJournalOfferRequirement() throws Exception {
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT-1);
    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.JOURNAL_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();

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

    // An offer with exactly the correct reserved resources should be accepted 
    offer = createReservedOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertTrue(constraint.canBeSatisfied(offer));

    // Offers with too much reserved resources should be rejected 
    offer = createReservedOfferBuilder(
        ENOUGH_JOURNAL_CPU+0.1,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createReservedOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM+1,
        ENOUGH_JOURNAL_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createReservedOfferBuilder(
        ENOUGH_JOURNAL_CPU,
        ENOUGH_JOURNAL_MEM,
        ENOUGH_JOURNAL_DISK+1,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.canBeSatisfied(offer));
  }

  @Test
  public void testSatisfiesJournalResourceOfferRequirements() throws Exception {
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT-1);
    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.JOURNAL_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();

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
  public void testSatisfiesJournalVolumeOfferRequirements() throws Exception {
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT-1);
    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.JOURNAL_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();

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

  @Test
  public void createNameOfferRequirement() throws Exception {
    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.NAME_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();
    assertTrue(constraint instanceof NameOfferRequirement);
  }

  @Test
  public void testCanSatisfyNameOfferRequirement() throws Exception {
    when(dnsResolver.journalNodesResolvable()).thenReturn(true);
    when(state.getNameCount()).thenReturn(TARGET_NAME_COUNT-1);
    when(state.hostOccupied(any(String.class), eq(HDFSConstants.JOURNAL_NODE_ID))).thenReturn(true);

    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.NAME_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();

    // An offer with enough resources should be accepted
    Offer offer = createOfferBuilder(ENOUGH_NAME_CPU, ENOUGH_NAME_MEM, ENOUGH_NAME_DISK).build();
    assertTrue(constraint.canBeSatisfied(offer));

    // Offers which lack the required resources of each type should be rejected
    offer = createOfferBuilder(ENOUGH_NAME_CPU-0.1, ENOUGH_NAME_MEM, ENOUGH_NAME_DISK).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createOfferBuilder(ENOUGH_NAME_CPU, ENOUGH_NAME_MEM-1, ENOUGH_NAME_DISK).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createOfferBuilder(ENOUGH_NAME_CPU, ENOUGH_NAME_MEM, ENOUGH_NAME_DISK-1).build();
    assertFalse(constraint.canBeSatisfied(offer));

    // An offer with exactly the correct reserved resources should be accepted 
    offer = createReservedOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertTrue(constraint.canBeSatisfied(offer));

    // Offers with too much reserved resources should be rejected 
    offer = createReservedOfferBuilder(
        ENOUGH_NAME_CPU+0.1,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createReservedOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM+1,
        ENOUGH_NAME_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.canBeSatisfied(offer));

    offer = createReservedOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK+1,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.canBeSatisfied(offer));
  }

  @Test
  public void testSatisfiesNameResourceOfferRequirements() throws Exception {
    when(dnsResolver.journalNodesResolvable()).thenReturn(true);
    when(state.getNameCount()).thenReturn(TARGET_NAME_COUNT-1);

    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.NAME_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();

    // An offer with enough resources which are not reserved should be rejected
    Offer offer = createOfferBuilder(ENOUGH_NAME_CPU, ENOUGH_NAME_MEM, ENOUGH_NAME_DISK).build();
    assertFalse(constraint.isSatisfiedForReservations(offer));

    // An offer with enough reserved resources should be accepted
    offer = createReservedOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK,
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
  public void testSatisfiesNameVolumeOfferRequirements() throws Exception {
    when(dnsResolver.journalNodesResolvable()).thenReturn(true);
    when(state.getNameCount()).thenReturn(TARGET_NAME_COUNT-1);

    OfferRequirementProvider provider = createOfferRequirementProvider(AcquisitionPhase.NAME_NODES);
    OfferRequirement constraint = provider.getNextOfferRequirement();

    // An offer with enough reserved resouces, but no volumes should be rejected
    Offer offer = createReservedOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK,
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.isSatisfiedForVolumes(offer));

    // An offer with enough reserved resources and the correct persistence ID should be accepted
    offer = createVolumeOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK,
        expectedVolume.getPersistenceId(),
        config.getRole(),
        config.getPrincipal()).build();
    assertTrue(constraint.isSatisfiedForVolumes(offer));

    // An offer with enough reserved resources and a volume with the wrong persistence ID should be rejected 
    offer = createVolumeOfferBuilder(
        ENOUGH_NAME_CPU,
        ENOUGH_NAME_MEM,
        ENOUGH_NAME_DISK,
        "bad-persistence-id",
        config.getRole(),
        config.getPrincipal()).build();
    assertFalse(constraint.isSatisfiedForVolumes(offer));
  }

  private OfferRequirementProvider createOfferRequirementProvider(AcquisitionPhase phase) { 
    return new OfferRequirementProvider(state, config, dnsResolver, phase, expectedVolume);
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
