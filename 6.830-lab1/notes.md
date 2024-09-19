# Tuple

A `Tuple` is a collection of `Field` objects, one per field in the tuple. The tuple descriptor, represented by a `TupleDesc` object describes the type of a tuple - it consists of a collection of `Type` objects, one per field in the tuple, each of which describes the type of the corresponding field.

# Field

A `Field` is an interface that different data types (e.g., integers, strings) implement.

# Catalog

A `Catalog` consists of a list of the tables and schemas of the tables (represented by `DbFile` objects) that are in the database. Each table corresponds to a `TupleDesc` that allows operators to determine the types and number of fields in a table.
