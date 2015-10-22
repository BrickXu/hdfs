package org.apache.mesos.hdfs.state; 
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.TaskID;

/**
 * VolumeRecord class encapsulates DiskInfo and TaskID metadata necessary for recording Persistent Volume state. 
 */
public class VolumeRecord implements Serializable {
  private DiskInfo info;
  private TaskID id; 

  public VolumeRecord(DiskInfo info, TaskID id) {
    this.info = info;
    this.id = id;
  }

  public DiskInfo getInfo() {
    return info;
  }

  public TaskID getTaskId() {
    return id;
  }

  public String getPersistenceId() {
    return info.getPersistence().getId();
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  private static class TaskDeserializationException extends ObjectStreamException {
  }

  private void readObjectNoData() throws ObjectStreamException {
    throw new TaskDeserializationException();
  }
}
