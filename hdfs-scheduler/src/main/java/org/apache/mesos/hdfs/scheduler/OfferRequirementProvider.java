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
  private VolumeRecord volume;

  @Inject
  public OfferRequirementProvider(
      HdfsState state,
      HdfsFrameworkConfig config,
      DnsResolver dnsResolver,
      VolumeRecord volume) {
    this.state = state;
    this.config = config;
    this.dnsResolver = dnsResolver;
    this.volume = volume;
  }

  public void setVolume(VolumeRecord volume) {
    this.volume = volume;
  }

  public OfferRequirement getNextOfferRequirement(AcquisitionPhase phase) throws Exception {
    switch(phase) {
      case JOURNAL_NODES:
        return new JournalOfferRequirement(state, config, volume);
      case NAME_NODES:
        return new NameOfferRequirement(state, config, dnsResolver, volume);
      case DATA_NODES:
        return new DataOfferRequirement(state, config, volume);
      default:
        throw new Exception("Unsupported Acquisition phase: " + phase);
    }
  }
}
