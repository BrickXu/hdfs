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
public class DataOfferRequirement extends AbstractOfferRequirement {
  private NodeConfig nodeConfig;

  public DataOfferRequirement(HdfsState state, HdfsFrameworkConfig config, VolumeRecord volume)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    super(state, config, volume, DataOfferRequirement.class);
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

  protected double getNeededCpus() {
    return nodeConfig.getCpus();
  }

  protected int getNeededMem() {
    return nodeConfig.getMaxHeap();
  }

  protected int getNeededDisk() {
    return nodeConfig.getDiskSize();
  }

  private boolean violatesExclusivityConstraint(Offer offer) {
    return config.getRunDatanodeExclusively() &&
      (state.hostOccupied(offer.getHostname(), HDFSConstants.NAME_NODE_ID)
        || state.hostOccupied(offer.getHostname(), HDFSConstants.JOURNAL_NODE_ID));
  }
}
