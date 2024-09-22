package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private class StringAggregatorIterator implements DbIterator {
        private StringAggregator stringAggregator;
        private Iterator<Entry<Field, Integer>> iterator;

        public StringAggregatorIterator(StringAggregator stringAggregator) {
            this.stringAggregator = stringAggregator;
        }

        public void open() throws DbException, TransactionAbortedException {
            this.iterator = this.stringAggregator.aggregatedCount.entrySet().iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.iterator == null) {
                return false;
            }

            return this.iterator.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (this.iterator == null) {
                throw new NoSuchElementException("next() is unsupported on an uninitialized iterator.");
            }

            Entry<Field, Integer> entry = this.iterator.next();
            Field groupByField = entry.getKey();
            int count = entry.getValue();

            if (groupByField == null) {
                Tuple tuple = new Tuple(
                    new TupleDesc(new Type[]{Type.INT_TYPE})
                );
                tuple.setField(0, new IntField(count));

                return tuple;
            } else {
                Tuple tuple = new Tuple(
                    new TupleDesc(new Type[]{this.stringAggregator.groupByType, Type.INT_TYPE})
                );
                tuple.setField(0, groupByField);
                tuple.setField(1, new IntField(count));

                return tuple;
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            this.iterator = this.stringAggregator.aggregatedCount.entrySet().iterator();
        }

        public TupleDesc getTupleDesc() {
            if (this.stringAggregator.groupByField == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{
                    this.stringAggregator.groupByType,
                    Type.INT_TYPE
                });
            }
        }

        public void close() {
            this.iterator = null;
        }
    }

    private int groupByField;
    private Type groupByType;
    private int aggregateField;
    private Op aggregationOperator;

    private HashMap<Field, Integer> aggregatedCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregationOperator = what;

        this.aggregatedCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field groupByField = this.groupByField == Aggregator.NO_GROUPING
            ? null
            : tup.getField(this.groupByField);
        
        this.aggregatedCount.put(
            groupByField,
            this.aggregatedCount.getOrDefault(groupByField, 0) + 1
        );
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new StringAggregatorIterator(this);
    }

}
