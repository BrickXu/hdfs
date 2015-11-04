package org.apache.mesos.hdfs.state;

/**
 * Defines node types.
 */
public enum AcquisitionPhase {

  /**
   * Performs task reconciliation 
   */
  RECONCILING_TASKS,

  /**
   * Launches and waits for all journalnodes to start.
   */
  JOURNAL_NODES,

  /**
   * Launches the namenodes.
   */
  NAME_NODES,

  /**
   * If everything is healthy the scheduler stays here and tries to launch
   * datanodes on any slave that doesn't have an hdfs task running on it.
   */
  DATA_NODES
}
