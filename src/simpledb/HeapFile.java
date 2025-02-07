package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order. Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    public class HeapFileIterator implements DbFileIterator {
        
        private HeapFile heapFile;
        private TransactionId transactionId;
        private Iterator<Tuple> iterator;
        private HeapPage heapPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId transactionId) {
            this.heapFile = heapFile;
            this.transactionId = transactionId;
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            this.heapPage = (HeapPage) Database.getBufferPool()
                .getPage(
                    transactionId,
                    new HeapPageId(heapFile.getId(), 0),
                    Permissions.READ_ONLY
                );
            this.iterator = this.heapPage.iterator();
        }

        /** @return true if there are more tuples available. */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.iterator == null) {
                return false;
            }

            if (this.iterator.hasNext()) {
                return true;
            }

            // Advance to the next page.
            while (this.heapPage.getId().pageno() + 1 < this.heapFile.numPages()) {
                int nextPageNum = this.heapPage.getId().pageno() + 1;
                this.heapPage = (HeapPage) Database.getBufferPool()
                    .getPage(
                        transactionId,
                        new HeapPageId(heapFile.getId(), nextPageNum),
                        Permissions.READ_ONLY
                    );
                this.iterator = this.heapPage.iterator();
                if (this.iterator.hasNext()) {
                    break;
                }
            }

            return this.iterator.hasNext();
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (this.iterator == null) {
                throw new NoSuchElementException("next() is unsupported on an uninitialized iterator.");
            }

            if (this.hasNext()) {
                return this.iterator.next();
            } else {
                throw new NoSuchElementException("There are no more tuples.");
            }
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            this.iterator = this.heapPage.iterator();
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.iterator = null;
        }
    }

    private File file;
    private TupleDesc tupleDesc;

    private Object pageAdder;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;

        this.pageAdder = new Object();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid == null) {
            throw new IllegalArgumentException("Page does not exist in the file.");
        }

        try {
            FileInputStream fileInputStream = new FileInputStream(this.file.getAbsoluteFile());
            int pageNum = pid.pageno();
            byte[] pageData = new byte[BufferPool.PAGE_SIZE];

            // Skip to the current page
            fileInputStream.skip(BufferPool.PAGE_SIZE * pageNum);
            // Read the current page
            fileInputStream.read(pageData);
            fileInputStream.close();

            return new HeapPage(new HeapPageId(this.getId(), pid.pageno()), pageData);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Page does not exist in the file.");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        randomAccessFile.seek(page.getId().pageno() * BufferPool.PAGE_SIZE);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(this.file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < this.numPages(); i++) {
            HeapPage readablePage = (HeapPage) Database
                .getBufferPool()
                .getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_ONLY);

            if (readablePage.getNumEmptySlots() == 0) {
                continue;
            }

            HeapPage writablePage = (HeapPage) Database
                .getBufferPool()
                .getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);

            writablePage.addTuple(t);
            writablePage.markDirty(true, tid);
            return new ArrayList<Page>(Arrays.asList(writablePage));
        }

        // Add a new page since no free pages are available.
        synchronized (pageAdder) {
            int pageNum = this.numPages();
            this.writePage(new HeapPage(
                new HeapPageId(this.getId(),pageNum),
                HeapPage.createEmptyPageData()
            ));
            HeapPage writablePage = (HeapPage) Database
                .getBufferPool()
                .getPage(tid, new HeapPageId(this.getId(), pageNum), Permissions.READ_WRITE);
            
            writablePage.addTuple(t);
            writablePage.markDirty(true, tid);
            return new ArrayList<Page>(Arrays.asList(writablePage));
        }
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        RecordId recordId = t.getRecordId();

        HeapPage page = (HeapPage) Database
            .getBufferPool()
            .getPage(
                tid,
                new HeapPageId(this.getId(), recordId.getPageId().pageno()),
                Permissions.READ_WRITE
            );
        
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
    
}

