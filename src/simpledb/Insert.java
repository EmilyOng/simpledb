package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    private TransactionId transactionId;
    private DbIterator childDbIterator;
    private int tableId;

    private boolean alreadyInserted;

    private static final TupleDesc TUPLE_DESCRIPTOR = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.transactionId = t;
        this.childDbIterator = child;
        this.tableId = tableid;

        this.alreadyInserted = false;

        TupleDesc childTupleDesc = this.childDbIterator.getTupleDesc();
        TupleDesc tableTupleDesc = Database.getCatalog().getTupleDesc(this.tableId);

        if (!childTupleDesc.equals(tableTupleDesc)) {
            throw new DbException("Tuple descriptor of child differs from table " + String.valueOf(this.tableId));
        }
    }

    public TupleDesc getTupleDesc() {
        return Insert.TUPLE_DESCRIPTOR;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (this.alreadyInserted) {
            return null;
        }

        int numInsertedRecords = 0;

        while (this.childDbIterator.hasNext()) {
            Tuple insertableTuple = this.childDbIterator.next();

            try {
                Database.getBufferPool()
                    .insertTuple(this.transactionId, this.tableId, insertableTuple);
                numInsertedRecords++;
            } catch (IOException e) {
                throw new DbException("Cannot insert tuple " + insertableTuple.toString() + " into the table " + String.valueOf(this.tableId) + ".");
            }
        }

        Tuple result = new Tuple(this.getTupleDesc());
        result.setField(0, new IntField(numInsertedRecords));

        this.alreadyInserted = true;

        return result;
    }
}
