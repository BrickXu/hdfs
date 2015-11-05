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
public class JournalOfferRequirement extends AbstractOfferRequirement {
  private NodeConfig nodeConfig;

  public JournalOfferRequirement(HdfsState state, HdfsFrameworkConfig config, VolumeRecord volume)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    super(state, config, volume, JournalOfferRequirement.class);
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

    if (!OfferRequirementUtils.enoughResources(
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

  protected double getNeededCpus() {
    return nodeConfig.getCpus();
  }

  protected int getNeededMem() {
    return nodeConfig.getMaxHeap();
  }

  protected int getNeededDisk() {
    return nodeConfig.getDiskSize();
  }
}
