package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;

import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.VolumeRecord;

/**
 *
 */
public class ConstraintProvider {
  private HdfsState state;
  private HdfsFrameworkConfig config;
  private AcquisitionPhase phase;
  private VolumeRecord volume;

  @Inject
  public ConstraintProvider(
      HdfsState state,
      HdfsFrameworkConfig config,
      AcquisitionPhase phase,
      VolumeRecord volume) {
    this.state = state;
    this.config = config;
    this.phase = phase;
    this.volume = volume;
  }

  public void setVolume(VolumeRecord volume) {
    this.volume = volume;
  }

  public Constraint getNextConstraint() throws Exception {
    switch(phase) {
      case JOURNAL_NODES:
        return new JournalConstraint(state, config, volume);
    }

    throw new Exception("Failed to create a valid Constraint");
  }
}
