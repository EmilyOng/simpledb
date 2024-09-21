package simpledb;

import java.util.NoSuchElementException;

/** Helper for implementing DbIterators. It handles <code>close</code>, <code>next</code> and
<code>hasNext</code>. Subclasses only need to implement <code>open</code> and
<code>readNext</code>. */
public abstract class AbstractDbIterator implements DbIterator {

    private Tuple next = null;

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.next == null) this.next = readNext();
        return this.next != null;
    }

    public Tuple next() throws
            DbException, TransactionAbortedException, NoSuchElementException {
        if (this.next == null) {
            this.next = readNext();
            if (this.next == null) {
                throw new NoSuchElementException();
            }
        }

        Tuple result = this.next;
        this.next = null;
        return result;
    }

    /** Returns the next Tuple in the iterator, or null if the iteration is
    finished. AbstractDbIterator uses this method to implement both
    <code>next</code> and <code>hasNext</code>.
    @return the next Tuple in the iterator, or null if the iteration is finished. */
    protected abstract Tuple readNext() throws DbException, TransactionAbortedException;

    /** Closes this iterator. If overridden by a subclass, they should call
     super.close() in order for AbstractDbIterator's internal state to be
    consistent. */
    public void close() {
        // Ensures that a future call to next() will fail
        this.next = null;
    }
}
