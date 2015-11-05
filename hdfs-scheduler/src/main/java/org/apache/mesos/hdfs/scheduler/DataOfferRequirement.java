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
public class DataOfferRequirement implements OfferRequirement {
  private final Log log = LogFactory.getLog(DataOfferRequirement.class);
  private HdfsState state;
  private HdfsFrameworkConfig config;
  private NodeConfig nodeConfig;
  private VolumeRecord volume;

  public DataOfferRequirement(HdfsState state, HdfsFrameworkConfig config, VolumeRecord volume)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    this.state = state;
    this.config = config;
    this.volume = volume;
    this.nodeConfig = config.getNodeConfig(HDFSConstants.DATA_NODE_ID);
  }

  public boolean canBeSatisfied(Offer offer) {
    boolean accept = false;

    if (!OfferRequirementUtils.enoughResources(
          offer,
          config,
          nodeConfig.getCpus(),
          nodeConfig.getMaxHeap(),
          nodeConfig.getDiskSize())) {
      log.info("Offer does not have enough resources");
    } else if (state.hostOccupied(offer.getHostname(), HDFSConstants.DATA_NODE_ID)) {
      log.info(String.format("Already running DataNode on %s", offer.getHostname()));
    } else if (violatesExclusivityConstraint(offer)) {
      log.info(String.format("Already running NameNode or JournalNode on %s", offer.getHostname()));
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

    double cpus = OfferRequirementUtils.getNeededCpus(nodeConfig.getCpus(), config);
    int mem = (int) OfferRequirementUtils.getNeededMem(nodeConfig.getMaxHeap(), config);
    int diskSize = nodeConfig.getDiskSize();

    if (!OfferRequirementUtils.cpuReserved(offer, cpus, role, principal)) {
      log.info("Offer does not have it's CPU resources reserved.");
    } else if (!OfferRequirementUtils.memReserved(offer, mem, role, principal)) {
      log.info("Offer does not have it's Memory resources reserved.");
    } else if (!OfferRequirementUtils.diskReserved(offer, diskSize, role, principal, expectedPersistenceId)) {
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

    List<Resource> reservedResources = OfferRequirementUtils.getScalarReservedResources(offer, "disk", diskSize, role, principal);

    for (Resource resource : reservedResources) {
      String actualPersistenceId = OfferRequirementUtils.getPersistenceId(resource);

      log.info(
          "Looking for persistence id: " + expectedPersistenceId +
          "; Found volume with persistence id: " + actualPersistenceId);

      if (expectedPersistenceId == actualPersistenceId) {
        return true;
      }
    }

    return false;
  }

  private boolean violatesExclusivityConstraint(Offer offer) {
    return config.getRunDatanodeExclusively() &&
      (state.hostOccupied(offer.getHostname(), HDFSConstants.NAME_NODE_ID)
        || state.hostOccupied(offer.getHostname(), HDFSConstants.JOURNAL_NODE_ID));
  }
}
