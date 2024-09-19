# Tuple

A `Tuple` is a collection of `Field` objects, one per field in the tuple. The tuple descriptor, represented by a `TupleDesc` object describes the type of a tuple - it consists of a collection of `Type` objects, one per field in the tuple, each of which describes the type of the corresponding field.

# Field

A `Field` is an interface that different data types (e.g., integers, strings) implement.
