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

public class NameOfferRequirement extends AbstractOfferRequirement {
  private final Log log = LogFactory.getLog(NameOfferRequirement.class);
  private DnsResolver dnsResolver;
  private NodeConfig nameConfig;
  private NodeConfig zkfcConfig;

  public NameOfferRequirement(
      HdfsState state,
      HdfsFrameworkConfig config,
      DnsResolver dnsResolver,
      VolumeRecord volume)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    super(state, config, volume, NameOfferRequirement.class);
    this.nameConfig = config.getNodeConfig(HDFSConstants.NAME_NODE_ID);
    this.zkfcConfig = config.getNodeConfig(HDFSConstants.ZKFC_NODE_ID);
    this.dnsResolver = dnsResolver;
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

  protected double getNeededCpus() {
    return nameConfig.getCpus() + zkfcConfig.getCpus();
  }

  protected int getNeededMem() {
    return nameConfig.getMaxHeap() + zkfcConfig.getMaxHeap();
  }
  
  protected int getNeededDisk() {
    return nameConfig.getDiskSize() + zkfcConfig.getDiskSize();
  }
}
