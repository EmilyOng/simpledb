1. Run `ant dist` to compile and build simpledb.
2. Convert `data.txt` to a SimpleDB table.
    ```bash
    java -jar dist/simpledb.jar convert data.txt 2 "int,int"
    ```

    This creates a file `data.dat`. In addition to the table's raw data, the two additional parameters specify that each record has two fields and that their types are `int` and `int`.
3. Invoke the parser
    ```bash
    java -jar dist/simpledb.jar parser examples/sample/catalog.txt
    ```

    The output is as follows:
    ```
    Added table : data with schema INT_TYPE(f1)INT_TYPE(f2)
    Computing table stats.
    Done.
    SimpleDB>
    ```
1. Run the query
    ```
    SimpleDB> select d.f1, d.f2 from data d;
    ```