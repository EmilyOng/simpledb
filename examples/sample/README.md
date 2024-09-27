1. Run `ant dist` to compile and build simpledb.
2. Change directory to the `examples/sample`.
3. Convert `data.txt` to a SimpleDB table.
    ```bash
    java -jar ../../dist/simpledb.jar convert data.txt 2 "int,int"
    ```

    This creates a file `data.dat`. In addition to the table's raw data, the two additional parameters specify that each record has two fields and that their types are `int` and `int`.
4. Invoke the parser
    ```bash
    java -jar ../../dist/simpledb.jar parser catalog.txt
    ```

    The output is as follows:
    ```
    Added table : data with schema INT_TYPE(f1), INT_TYPE(f2)
    Computing table stats.
    Done.
    SimpleDB>
    ```
5. Run the query
    ```
    SimpleDB> select d.f1, d.f2 from data d;
    ```

    The output is as follows:
    ```
    SimpleDB> select d.f1, d.f2 from data d;
    Added scan of table d
    Added select list field d.f1
    Added select list field d.f2
    f1      f2
    --------------
    1       10
    2       20
    3       30
    4       40
    5       50
    5       50

    6 rows.
    ----------------
    0.04 seconds

    SimpleDB> 
    ```