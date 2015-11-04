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
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

public class NameOfferRequirement implements OfferRequirement {
  private final Log log = LogFactory.getLog(NameOfferRequirement.class);
  private HdfsState state;
  private HdfsFrameworkConfig config;
  private DnsResolver dnsResolver;
  private NodeConfig nameConfig;
  private NodeConfig zkfcConfig;
  private VolumeRecord volume;

  public NameOfferRequirement(
      HdfsState state,
      HdfsFrameworkConfig config,
      DnsResolver dnsResolver,
      VolumeRecord volume)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    this.state = state;
    this.config = config;
    this.dnsResolver = dnsResolver;
    this.volume = volume;
    this.nameConfig = config.getNodeConfig(HDFSConstants.NAME_NODE_ID);
    this.zkfcConfig = config.getNodeConfig(HDFSConstants.ZKFC_NODE_ID);
  }

  private double getNeededCpus() {
    return nameConfig.getCpus() + zkfcConfig.getCpus();
  }

  private int getNeededMem() {
    return nameConfig.getMaxHeap() + zkfcConfig.getMaxHeap();
  }
  
  private int getNeededDisk() {
    return nameConfig.getDiskSize() + zkfcConfig.getDiskSize();
  }

  public boolean canBeSatisfied(Offer offer) {
    boolean accept = false;
    String hostname = offer.getHostname();

    if (dnsResolver.journalNodesResolvable()) {
      int nameCount = 0;

      try {
        nameCount = state.getNameCount();
      } catch (Exception ex) {
        log.error("Failed to retrieve NameNode count with exception: " + ex);
        return false;
      }

      if (!OfferRequirementUtils.enoughResources(
            offer,
            config,
            getNeededCpus(),
            getNeededMem(),
            getNeededDisk())) {
        log.info("Offer does not have enough resources");
      } else if (nameCount >= HDFSConstants.TOTAL_NAME_NODES) {
        log.info(String.format("Already running %s namenodes", HDFSConstants.TOTAL_NAME_NODES));
      } else if (state.hostOccupied(hostname, HDFSConstants.NAME_NODE_ID)) {
        log.info(String.format("Already running namenode on %s", offer.getHostname()));
      } else if (state.hostOccupied(hostname, HDFSConstants.DATA_NODE_ID)) {
        log.info(String.format("Cannot colocate namenode and datanode on %s", offer.getHostname()));
      } else if (!state.hostOccupied(hostname, HDFSConstants.JOURNAL_NODE_ID)) {
        log.info(String.format("We need to colocate the namenode with a journalnode and there is "
          + "no journalnode running on this host. %s", offer.getHostname()));
      } else {
        accept = true;
      }
    }

    return accept;
  }

  public boolean isSatisfiedForReservations(Offer offer) {
    log.info("Offer: " + offer);
    boolean accept = false;

    String role = config.getRole();
    String principal = config.getPrincipal();
    String expectedPersistenceId = volume.getPersistenceId();

    double cpus = OfferRequirementUtils.getNeededCpus(getNeededCpus(), config);
    int mem = (int) OfferRequirementUtils.getNeededMem(getNeededMem(), config);
    int diskSize = getNeededDisk();

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
    int diskSize = getNeededDisk();
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
}
