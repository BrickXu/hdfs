package org.apache.mesos.hdfs.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

/**
 * 
 */
public class ConstraintUtils {
  private static final Log log = LogFactory.getLog(ConstraintUtils.class);

  public static boolean enoughResources(
      Offer offer,
      HdfsFrameworkConfig config,
      double cpus,
      int mem,
      int disk) {

    for (Resource resource : offer.getResourcesList()) {
      String resourceName = resource.getName();
      double availableValue = resource.getScalar().getValue();

      switch (resourceName) {
        case "cpus":
          double neededCpus = getNeededCpus(cpus, config);
          log.info("Needed CPUs: " + neededCpus + " Available CPUs: " + availableValue);

          if (neededCpus > availableValue) {
            log.warn("Not enough CPU resources");
            return false;
          }
          break;

        case "mem":
          double neededMem = getNeededMem(mem, config);
          log.info("Needed Memory: " + neededMem + " Available Memory: " + availableValue);

          if (neededMem > availableValue) {
            log.warn("Not enough Memory resources");
            return false;
          }
          break;

       case "disk":
          log.info("Needed Disk: " + disk + " Available disk: " + availableValue);

          if (disk > availableValue) {
            log.warn("Not enough Disk resources");
            return false;
          }
          break;
      }
    }

    return true;
  }

  public static double getNeededMem(double mem, HdfsFrameworkConfig config) {
    double neededMem = (mem * config.getJvmOverhead())
      + (config.getExecutorHeap() * config.getJvmOverhead());

    return (int) neededMem;
  }

  public static double getNeededCpus(double cpus, HdfsFrameworkConfig config) {
    return cpus + config.getExecutorCpus();
  }

  private static boolean scalarResourceReserved(Offer offer, String resourceName, double value, String role, String principal) {
    return getScalarReservedResources(offer, resourceName, value, role, principal).size() > 0;
  }

  public static List<Resource> getScalarReservedResources(
      Offer offer,
      String resourceName,
      double value,
      String role,
      String principal) {

    List<Resource> reservedResources = new ArrayList<Resource>();
    for (Resource resource : offer.getResourcesList()) {
      String resPrincipal = resource.getReservation().getPrincipal();
      String resRole = resource.getRole();
      String resName = resource.getName();
      double resValue = resource.getScalar().getValue();

      if (resName.equals(resourceName) &&
          resPrincipal.equals(principal) &&
          resRole.equals(role) &&
          resValue == value) {
        reservedResources.add(resource);
      }
    }

    return reservedResources; 
  }

  public static boolean cpuReserved(Offer offer, double cpus, String role, String principal) {
    return scalarResourceReserved(offer, "cpus", cpus, role, principal);
  }

  public static boolean memReserved(Offer offer, double mem, String role, String principal) {
    return scalarResourceReserved(offer, "mem", mem, role, principal);
  }

  public static boolean diskReserved(Offer offer, double diskSize, String role, String principal, String persistenceId) {
    List<Resource> reservedResources = getScalarReservedResources(offer, "disk", diskSize, role, principal);
    for (Resource resource : reservedResources) {
      String actualPersistenceId = getPersistenceId(resource);
      if (actualPersistenceId.equals(persistenceId) || actualPersistenceId.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  public static boolean volumeCreated(Offer offer, double diskSpace, String role, String principal, String persistenceId) {
    return false;
  }

  public static String getPersistenceId(Resource resource) {
    return resource.getDisk().getPersistence().getId();
  }

  public static List<String> getPersistenceIds(Offer offer) {
    List<String> persistenceIds = new ArrayList<String>();
    for (Resource resource : offer.getResourcesList()) {
      String persistenceId = getPersistenceId(resource);
      if (persistenceId != null && !persistenceId.isEmpty()) {
        persistenceIds.add(persistenceId);
      }
    }

    return persistenceIds;
  }
}
