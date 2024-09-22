package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    private DbIterator childIterator;
    private int aggregateField;
    private int groupField;
    private Aggregator.Op aggregationOperator;

    private Aggregator aggregator;
    private DbIterator aggregatorIterator;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.childIterator = child;
        this.aggregateField = afield;
        this.groupField = gfield;
        this.aggregationOperator = aop;

        TupleDesc tupleDesc = this.childIterator.getTupleDesc();
        switch (tupleDesc.getType(this.aggregateField)) {
            case INT_TYPE:
                this.aggregator = new IntAggregator(
                    this.groupField,
                    this.groupField == Aggregator.NO_GROUPING
                        ? null
                        : tupleDesc.getType(this.groupField),
                    this.aggregateField,
                    this.aggregationOperator
                );
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(
                    this.groupField,
                    this.groupField == Aggregator.NO_GROUPING
                        ? null
                        : tupleDesc.getType(this.groupField),
                    this.aggregateField,
                    this.aggregationOperator
                );
                break;
            default:
                break;
        }
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        this.childIterator.open();
        while (this.childIterator.hasNext()) {
            this.aggregator.merge(this.childIterator.next());
        }
        this.aggregatorIterator = this.aggregator.iterator();
        this.aggregatorIterator.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (this.aggregatorIterator.hasNext()) {
            return this.aggregatorIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggregatorIterator.rewind();
    }

    private String getAggregationFieldName() {
        return Aggregate.aggName(this.aggregationOperator) + " " + "(" + this.aggregateField + ")";
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = this.childIterator.getTupleDesc();

        if (this.groupField == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                new Type[]{
                    tupleDesc.getType(this.groupField),
                    tupleDesc.getType(this.aggregateField)
                },
                new String[]{
                    tupleDesc.getFieldName(this.groupField),
                    this.getAggregationFieldName()
                }
            );
        } else {
            return new TupleDesc(
                new Type[]{
                    tupleDesc.getType(this.groupField),
                    tupleDesc.getType(this.aggregateField)
                },
                new String[]{
                    tupleDesc.getFieldName(this.groupField),
                    this.getAggregationFieldName()
                }
            );
        }
    }

    public void close() {
        this.aggregatorIterator.close();
        this.childIterator.close();
    }
}
