package org.tikv.flink.connectors.coordinators;

public interface SnapshotCoordinator extends AutoCloseable {
  Transaction openTransaction(long checkpointId);

  Transaction getTransaction(long checkpointId);

  Transaction prewriteTransaction(long checkpointId, byte[] proposedKey);

  Transaction commitTransaction(long checkpointId);

  Transaction abortTransaction(long checkpointId);
}
