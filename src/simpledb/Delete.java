package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    private TransactionId transactionId;
    private DbIterator childDbIterator;

    private boolean alreadyDeleted;

    private static final TupleDesc TUPLE_DESCRIPTOR = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.transactionId = t;
        this.childDbIterator = child;

        this.alreadyDeleted = false;
    }

    public TupleDesc getTupleDesc() {
        return Delete.TUPLE_DESCRIPTOR;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.childDbIterator.open();
    }

    public void close() {
        this.childDbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.childDbIterator.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (this.alreadyDeleted) {
            return null;
        }

        int numDeletedRecords = 0;

        while (this.childDbIterator.hasNext()) {
            Tuple deleteableTuple = this.childDbIterator.next();

            Database.getBufferPool().deleteTuple(this.transactionId, deleteableTuple);
            numDeletedRecords++;
        }

        Tuple result = new Tuple(this.getTupleDesc());
        result.setField(0, new IntField(numDeletedRecords));

        this.alreadyDeleted = true;

        return result;
    }
}
