package simpledb;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {

    private TupleDesc tupleDesc;
    private Field[] fields;
    private RecordId recordId;

    /**
     * Merge two Tuples into one, with t1.getTupleDesc().numFields +
     * t2.getTupleDesc().numFields fields, with the first t1.getTupleDesc().numFields
     * coming from t1 and the remaining from t2.
     * @param t1 The Tuple with the first fields of the new Tuple
     * @param t2 The Tuple with the last fields of the Tuple
     * @return the new Tuple
     */
    public static Tuple combine(Tuple t1, Tuple t2) {
        TupleDesc td1 = t1.getTupleDesc();
        TupleDesc td2 = t2.getTupleDesc();

        Tuple combinedTuple = new Tuple(TupleDesc.combine(td1, td2));
        for (int i = 0; i < td1.numFields(); i++) {
            combinedTuple.setField(i, t1.getField(i));
        }
        for (int i = 0; i < td2.numFields(); i++) {
            combinedTuple.setField(i + td1.numFields(), t2.getField(i));
        }

        return combinedTuple;
    }

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td.numFields() == 0) {
            throw new IllegalArgumentException("Tuple descriptor must have at least one field.");
        }

        this.tupleDesc = td;
        this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= this.tupleDesc.numFields()) {
            throw new IllegalArgumentException("Field index " + String.valueOf(i) + " is invalid.");
        }
        
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if (i < 0 || i >= this.tupleDesc.numFields()) {
            throw new IllegalArgumentException("Field index " + String.valueOf(i) + " is invalid.");
        }
        
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String tupleString = "";
        for (int i = 0; i < this.fields.length; i++) {
            tupleString += this.fields[i].toString();
            if (i + 1 < this.fields.length) {
                tupleString += "\t";
            }
        }
        tupleString += "\n";
        return tupleString;
    }
}
