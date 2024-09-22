package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    private class IntAggregatorIterator implements DbIterator {
        private IntAggregator intAggregator;
        private Iterator<Entry<Field, AggregateValue>> iterator;

        public IntAggregatorIterator(IntAggregator intAggregator) {
            this.intAggregator = intAggregator;
        }

        public void open() throws DbException, TransactionAbortedException {
            this.iterator = this.intAggregator.aggregatedValues.entrySet().iterator();
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

            Entry<Field, AggregateValue> entry = this.iterator.next();
            Field groupByField = entry.getKey();
            AggregateValue aggregatedValue = entry.getValue();

            if (groupByField == null) {
                Tuple tuple = new Tuple(
                    new TupleDesc(new Type[]{Type.INT_TYPE})
                );
                tuple.setField(0, new IntField(aggregatedValue.getValue()));

                return tuple;
            } else {
                Tuple tuple = new Tuple(
                    new TupleDesc(new Type[]{this.intAggregator.groupByType, Type.INT_TYPE})
                );
                tuple.setField(0, groupByField);
                tuple.setField(1, new IntField(aggregatedValue.getValue()));

                return tuple;
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            this.iterator = this.intAggregator.aggregatedValues.entrySet().iterator();
        }

        public TupleDesc getTupleDesc() {
            if (this.intAggregator.groupByField == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{
                    this.intAggregator.groupByType,
                    Type.INT_TYPE
                });
            }
        }

        public void close() {
            this.iterator = null;
        }
    }

    private class AggregateValue {
        private int value;
        private int count;

        private Op aggregationOperator;

        public AggregateValue(Op aggregationOperator) {
            this.count = 0;
            this.aggregationOperator = aggregationOperator;
        }

        public void updateValue(int value) {
            if (this.count == 0) {
                this.value = value;
                this.count++;
                return;
            }

            switch (this.aggregationOperator) {
                case MIN:
                    this.value = Math.min(this.value, value);
                    break;
                case MAX:
                    this.value = Math.max(this.value, value);
                    break;
                case SUM:
                case AVG:
                    this.value += value;
                    break;
                default:
                    break;
            }

            this.count++;
        }

        private int getValue() {
            switch (this.aggregationOperator) {
                case MIN:
                case MAX:
                case SUM:
                    return this.value;
                case COUNT:
                    return this.count;
                case AVG:
                    return this.value / this.count;
                default:
                    break;
            }
            return this.value;
        }
    }


    private int groupByField;
    private Type groupByType;
    private int aggregateField;
    private Op aggregationOperator;

    private HashMap<Field, AggregateValue> aggregatedValues;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregationOperator = what;

        this.aggregatedValues = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field groupByField = this.groupByField == Aggregator.NO_GROUPING
            ? null
            : tup.getField(this.groupByField);
        IntField field = (IntField) tup.getField(this.aggregateField);

        if (!this.aggregatedValues.containsKey(groupByField)) {
            this.aggregatedValues.put(
                groupByField, new AggregateValue(this.aggregationOperator)
            );
        }
        
        this.aggregatedValues.get(groupByField)
            .updateValue(field.getValue());
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
        return new IntAggregatorIterator(this);
    }

}
