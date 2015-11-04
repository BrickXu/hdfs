package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;

import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.VolumeRecord;
import org.apache.mesos.hdfs.util.DnsResolver;

/**
 *
 */
public class OfferRequirementProvider {
  private HdfsState state;
  private HdfsFrameworkConfig config;
  private DnsResolver dnsResolver;
  private AcquisitionPhase phase;
  private VolumeRecord volume;

  @Inject
  public OfferRequirementProvider(
      HdfsState state,
      HdfsFrameworkConfig config,
      DnsResolver dnsResolver,
      AcquisitionPhase phase,
      VolumeRecord volume) {
    this.state = state;
    this.config = config;
    this.dnsResolver = dnsResolver;
    this.phase = phase;
    this.volume = volume;
  }

  public void setVolume(VolumeRecord volume) {
    this.volume = volume;
  }

  public OfferRequirement getNextOfferRequirement() throws Exception {
    switch(phase) {
      case JOURNAL_NODES:
        return new JournalOfferRequirement(state, config, volume);
      case NAME_NODES:
        return new NameOfferRequirement(state, config, dnsResolver, volume);
    }

    throw new Exception("Failed to create a valid OfferRequirement");
  }
}
