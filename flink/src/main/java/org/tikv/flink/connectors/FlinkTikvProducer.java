package org.tikv.flink.connectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.TiConfiguration;
import org.tikv.common.codec.TiTableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.flink.connectors.coordinators.SnapshotCoordinator;
import org.tikv.flink.connectors.coordinators.Transaction;
import org.tikv.txn.TwoPhaseCommitter;
import shade.com.google.common.base.Preconditions;

public class FlinkTikvProducer extends RichSinkFunction<RowData>
    implements CheckpointedFunction, CheckpointListener {

  private static final long serialVersionUID = 1L;

  private Logger logger = LoggerFactory.getLogger(FlinkTikvProducer.class);

  private final TiConfiguration conf;
  private final TiTableInfo tableInfo;
  private final FieldGetter[] fieldGetters;
  private final int pkIndex;

  private transient SnapshotCoordinator coordinator = null;
  private transient volatile TransactionHolder currentTransaction = null;
  private final transient List<BytePairWrapper> cachedValues = new ArrayList<>();
  private final transient Map<Long, TransactionHolder> transactionMap = new HashMap<>();

  // transactions
  protected transient ListState<Transaction> transactionState;

  public FlinkTikvProducer(
      final TiConfiguration conf, final TiTableInfo tableInfo, final DataType dataType) {
    this.conf = conf;
    this.tableInfo = tableInfo;

    final List<LogicalType> colTypes = dataType.getLogicalType().getChildren();
    fieldGetters = new FieldGetter[colTypes.size()];
    for (int i = 0; i < fieldGetters.length; i++) {
      fieldGetters[i] = TypeUtils.createFieldGetter(colTypes.get(i), i);
    }
    logger.info("colTypes: {}", colTypes);

    Optional<TiColumnInfo> pk =
        tableInfo.getColumns().stream().filter(TiColumnInfo::isPrimaryKey).findFirst();
    Preconditions.checkArgument(pk.isPresent() && TypeUtils.isIntType(pk.get().getType()));

    this.pkIndex = tableInfo.getColumns().indexOf(pk.get());
  }

  @Override
  public void open(final Configuration config) throws Exception {
    logger.info("open sink");
    super.open(config);
  }

  @Override
  public void invoke(final RowData row, final Context context) throws Exception {
    final BytePairWrapper kv = encodeRow(row);

    if (currentTransaction.getStatus() == Transaction.Status.NEW) {
      prewritePrimaryKey(kv);
    }

    cachedValues.add(kv);

    if (cachedValues.size() >= conf.getScanBatchSize()) {
      prewrite();
    }
  }

  public void prewritePrimaryKey(final BytePairWrapper kv) {
    final byte[] pk = Objects.isNull(kv) ? new byte[0] : kv.getKey();

    final Transaction txn =
        coordinator.prewriteTransaction(currentTransaction.getCheckpointId(), pk);

    if (Arrays.equals(txn.getPrimaryKey(), pk) && pk.length > 0) {
      final TwoPhaseCommitter committer = new TwoPhaseCommitter(conf, txn.getStartTs());
      committer.prewritePrimaryKey(ConcreteBackOffer.newRawKVBackOff(), kv.getKey(), kv.getValue());
      currentTransaction = new TransactionHolder(txn, committer);
    } else {
      currentTransaction = new TransactionHolder(txn);
    }
  }

  protected void prewrite() {
    if (cachedValues.isEmpty()) {
      return;
    }

    currentTransaction
        .getCommitter()
        .prewriteSecondaryKeys(currentTransaction.getPrimaryKey(), cachedValues.iterator(), 1000);
    cachedValues.forEach(kv -> currentTransaction.addSecondaryKey(new ByteWrapper(kv.getKey())));

    cachedValues.clear();
  }

  protected void commit(final Transaction txn, final TwoPhaseCommitter committer) {
    final byte[] pk = txn.getPrimaryKey();
    if (Objects.nonNull(pk) && pk.length > 0) {
      committer.commitPrimaryKey(ConcreteBackOffer.newRawKVBackOff(), pk, txn.getCommitTs());
    }
    coordinator.commitTransaction(txn.getCheckpointId());
  }

  protected void commitSecondaryKeys(
      final Transaction txn,
      final TwoPhaseCommitter committer,
      final Iterator<ByteWrapper> secondaryKeys)
      throws InterruptedException {
    while (true) {
      final Transaction t = coordinator.getTransaction(txn.getCheckpointId());
      if (t.getStatus() == Transaction.Status.COMMITTED) {
        committer.commitSecondaryKeys(secondaryKeys, t.getCommitTs(), 1000);
        return;
      }

      Thread.sleep(200);
    }
  }

  public BytePairWrapper encodeRow(final RowData row) {
    final Object pkValue = fieldGetters[pkIndex].getFieldOrNull(row);
    long handle = 0;
    if (pkValue instanceof Long) {
      handle = ((Long) pkValue).longValue();
    } else {
      handle = ((Integer) pkValue).longValue();
    }
    final RowKey rowKey = RowKey.toRowKey(tableInfo.getId(), handle);
    if (row.getRowKind() == RowKind.DELETE) {
      return new BytePairWrapper(rowKey.getBytes(), new byte[0]);
    } else {
      try {
        return new BytePairWrapper(
            rowKey.getBytes(),
            TiTableCodec.encodeRow(
                tableInfo.getColumns(), TypeUtils.toObjects(row, fieldGetters), true, true));
      } catch (final Throwable t) {
        logger.error("failed to encode row", t);
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public void close() throws Exception {
    coordinator.close();
  }

  @Override
  public void notifyCheckpointComplete(final long checkpointId) throws Exception {
    final TransactionHolder txn = transactionMap.remove(checkpointId);
    if (txn != null) {
      if (this.getRuntimeContext().getIndexOfThisSubtask() == 0) {
        commit(txn.getTransaction(), txn.getCommitter());
      }
      // TODO: run in background
      commitSecondaryKeys(txn.getTransaction(), txn.getCommitter(), txn.getSecondaryKeys());
    }
  }

  @Override
  public void snapshotState(final FunctionSnapshotContext context) throws Exception {
    prewrite();

    transactionState.clear();
    transactionState.add(currentTransaction.getTransaction());
    transactionMap.put(context.getCheckpointId(), currentTransaction);

    currentTransaction =
        new TransactionHolder(coordinator.openTransaction(context.getCheckpointId()));
  }

  @Override
  public void initializeState(final FunctionInitializationContext context) throws Exception {
    transactionState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>("transactionState", TransactionSerializer.INSTANCE));

    for (final Transaction txn : transactionState.get()) {
      // TODO: recover or abort transaction
    }

    currentTransaction = new TransactionHolder(coordinator.openTransaction(0));

    transactionState.add(currentTransaction.getTransaction());
  }

  static class TransactionHolder implements Transaction {
    public static final int SECONDARY_KEYS_LIMIT = (1 << 22);
    private final Transaction transaction;
    private final TwoPhaseCommitter committer;
    private final ArrayList<ByteWrapper> secondaryKeys = new ArrayList<>();

    TransactionHolder(final Transaction transaction) {
      this(transaction, null);
    }

    TransactionHolder(final Transaction transaction, final TwoPhaseCommitter committer) {
      this.transaction = transaction;
      this.committer = committer;
    }

    public void addSecondaryKey(final ByteWrapper key) {
      secondaryKeys.add(key);
    }

    public Transaction getTransaction() {
      return transaction;
    }

    public TwoPhaseCommitter getCommitter() {
      return committer;
    }

    public Iterator<ByteWrapper> getSecondaryKeys() {
      return secondaryKeys.iterator();
    }

    @Override
    public long getCheckpointId() {
      return transaction.getCheckpointId();
    }

    @Override
    public long getStartTs() {
      return transaction.getStartTs();
    }

    @Override
    public long getCommitTs() {
      return transaction.getCommitTs();
    }

    @Override
    public byte[] getPrimaryKey() {
      return transaction.getPrimaryKey();
    }

    @Override
    public Status getStatus() {
      return transaction.getStatus();
    }
  }
}
