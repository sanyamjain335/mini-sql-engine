# Mini-SQL Engine

  - ***Mini-SQL*** engine in ***Python*** which will run a subset of SQL Queries using command line interface.
  - ## **Dataset**
      Files used - 
      1) metadata.txt - information about the tables
      2) table[n].csv - entries in the table
    
  - ## **Queries**
    - **Select** all records: ``` SELECT * FROM table_name; ```
    - **Aggregate** functions: ***sum, average, min, max*** ``` SELECT MAX(col) FROM table_name; ```
    - **Project** coloumns: ``` SELECT col FROM table_name; ```
    - Project with **distinct**: ``` SELECT DISTINCT col FROM table_name; ```
    - Select from **one or more tables**: ``` SELECT col1, col2 FROM table1, table2 WHERE col1=10 AND col2=20; ```
    - Projection from one or more from two tables with **one join condition**: 
    ```
    a. SELECT * FROM table1, table2 WHERE table1.col1=table2.col2;
    b. SELECT col1, col2 FROM table1, table2 WHERE table1.col1=table2.col2; 
    ```
    - Basic Error Handling is done
  - ## **Execute**
    - run SQL-Engine from **python file** ``` python 2020201006.py <query> ```
