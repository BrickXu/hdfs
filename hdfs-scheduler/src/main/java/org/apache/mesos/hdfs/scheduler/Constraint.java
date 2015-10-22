package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;

/**
 *
 */
public interface Constraint {
  public boolean canBeSatisfied(Offer offer);
  public boolean isSatisfiedForReservations(Offer offer);
  public boolean isSatisfiedForVolumes(Offer offer);
}
