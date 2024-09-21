# Tuple

A `Tuple` is a collection of `Field` objects, one per field in the tuple. The tuple descriptor, represented by a `TupleDesc` object describes the type of a tuple - it consists of a collection of `Type` objects, one per field in the tuple, each of which describes the type of the corresponding field.

# Field

A `Field` is an interface that different data types (e.g., integers, strings) implement.

# Catalog

A `Catalog` consists of a list of the tables and schemas of the tables (represented by `DbFile` objects) that are in the database. Each table corresponds to a `TupleDesc` that allows operators to determine the types and number of fields in a table.

# Buffer Pool

The `BufferPool` is responsible for caching pages in memory that have been recently read from the disk. All operators read and write pages from various files on disk through the buffer pool.

# Heap File

A `HeapFile` object is arranged into a set of pages, each of which consists of a fixed number of bytes for storing tuples, including a header.
- Each table in the database corresponds to a single `HeapFile.
- Each page in a `HeapFile` is arranged as a set of slots, each of which can hold a single tuple (*tuples for a given table in SimpleDB are assumed to be of the same size*), and a header (consisting of a bitmap with one bit per tuple slot).
