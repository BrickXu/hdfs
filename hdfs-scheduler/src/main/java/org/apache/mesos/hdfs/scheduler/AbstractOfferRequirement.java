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
public abstract class AbstractOfferRequirement implements OfferRequirement {
  protected HdfsState state;
  protected HdfsFrameworkConfig config;
  protected VolumeRecord volume;
  protected Log log;

  public AbstractOfferRequirement(
      HdfsState state,
      HdfsFrameworkConfig config,
      VolumeRecord volume, 
      Class subClass)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    this.state = state;
    this.config = config;
    this.volume = volume;
    this.log = LogFactory.getLog(subClass);
  }

  public boolean isSatisfiedForReservations(Offer offer) {
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

  protected abstract double getNeededCpus();
  protected abstract int getNeededMem();
  protected abstract int getNeededDisk();
}
