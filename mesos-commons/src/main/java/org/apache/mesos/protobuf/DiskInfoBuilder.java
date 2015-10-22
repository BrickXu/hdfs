package org.apache.mesos.protobuf;

import org.apache.mesos.Protos.Resource.DiskInfo;

/**
 * Builder class for working with protobufs.  It includes 2 different approaches;
 * 1) static functions useful for developers that want helpful protobuf functions for CommandInfo.
 * 2) builder class
 * All builder classes provide access to the protobuf builder for capabilities beyond the included
 * helpful functions.
 * <p/>
 * This builds DiskInfo objects.
 */
public class DiskInfoBuilder {
  private DiskInfo.Builder builder = DiskInfo.newBuilder();

  public DiskInfo.Builder setPersistence(String persistenceId) {
    return builder.setPersistence(
        DiskInfo.Persistence.newBuilder()
        .setId(persistenceId).build());
  }

  public DiskInfo build() {
    return builder.build();
  }

  public DiskInfo.Builder builder() {
    return builder;
  }
}
