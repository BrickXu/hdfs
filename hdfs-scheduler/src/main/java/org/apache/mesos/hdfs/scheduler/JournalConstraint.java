package org.apache.mesos.hdfs.scheduler;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.VolumeRecord;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

/**
 *
 */
public class JournalConstraint implements Constraint {
  private final Log log = LogFactory.getLog(JournalConstraint.class);
  private HdfsState state;
  private HdfsFrameworkConfig config;
  private NodeConfig nodeConfig;
  private VolumeRecord volume;

  public JournalConstraint(HdfsState state, HdfsFrameworkConfig config, VolumeRecord volume)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    this.state = state;
    this.config = config;
    this.volume = volume;
    this.nodeConfig = config.getNodeConfig(HDFSConstants.JOURNAL_NODE_ID);
  }

  private VolumeRecord getFirstOrphanedJournalVolume()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {

    List<VolumeRecord> orphanedVolumes = state.getOrphanedVolumes(HDFSConstants.JOURNAL_NODE_ID);

    if (orphanedVolumes.size() > 0) {
      return orphanedVolumes.get(0);
    } else {
      return null;
    }
  }

  public boolean canBeSatisfied(Offer offer) {
    boolean accept = false;
    int journalCount = 0;

    try {
      journalCount = state.getJournalCount();
    } catch (Exception ex) {
      log.error("Failed to retrieve Journal count with exception: " + ex);
      return false;
    }

    if (!ConstraintUtils.enoughResources(
          offer,
          config,
          nodeConfig.getCpus(),
          nodeConfig.getMaxHeap(),
          nodeConfig.getDiskSize())) {
      log.info("Offer does not have enough resources");
    } else if (journalCount >= config.getJournalNodeCount()) {
      log.info(String.format("Already running %s journalnodes", config.getJournalNodeCount()));
    } else if (state.hostOccupied(offer.getHostname(), HDFSConstants.JOURNAL_NODE_ID)) {
      log.info(String.format("Already running journalnode on %s", offer.getHostname()));
    } else if (state.hostOccupied(offer.getHostname(), HDFSConstants.DATA_NODE_ID)) {
      log.info(String.format("Cannot colocate journalnode and datanode on %s", offer.getHostname()));
    } else {
      accept = true;
    }

    return accept;
  }

  public boolean isSatisfiedForReservations(Offer offer) {
    boolean accept = false;

    String role = config.getRole();
    String principal = config.getPrincipal();
    String expectedPersistenceId = volume.getPersistenceId();

    double cpus = ConstraintUtils.getNeededCpus(nodeConfig.getCpus(), config);
    int mem = (int) ConstraintUtils.getNeededMem(nodeConfig.getMaxHeap(), config);
    int diskSize = nodeConfig.getDiskSize();

    if (!ConstraintUtils.cpuReserved(offer, cpus, role, principal)) {
      log.info("Offer does not have it's CPU resources reserved.");
    } else if (!ConstraintUtils.memReserved(offer, mem, role, principal)) {
      log.info("Offer does not have it's Memory resources reserved.");
    } else if (!ConstraintUtils.diskReserved(offer, diskSize, role, principal, expectedPersistenceId)) {
      log.info("Offer does not have it's Disk resources reserved.");
    } else {
      accept = true;
    }

    return accept;
  }

  public boolean isSatisfiedForVolumes(Offer offer) {
    int diskSize = nodeConfig.getDiskSize();
    String role = config.getRole();
    String principal = config.getPrincipal();
    String expectedPersistenceId = volume.getPersistenceId();

    List<Resource> reservedResources = ConstraintUtils.getScalarReservedResources(offer, "disk", diskSize, role, principal);

    for (Resource resource : reservedResources) {
      String actualPersistenceId = ConstraintUtils.getPersistenceId(resource);

      log.info(
          "Looking for persistence id: " + expectedPersistenceId +
          "; Found volume with persistence id: " + actualPersistenceId);

      if (expectedPersistenceId == actualPersistenceId) {
        return true;
      }
    }

    return false;
  }
}
