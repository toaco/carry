# dbporter

## Summary

A python library to migrate data between different table in different database.

## Feature

- Migrating data from many source database to one destination database.
  - The database table structures can be completely different between source databases and destination database.
  - write SQL base on a previously migrated table to migrate subsequent table.
  - execute initial SQL after migration has been finished.

## Example

Suppose there are three databases: `sdb1`(sdb is the abbreviation of source database), `sdb2`  and `ddb`(the abbreviation of destination database). We want to migrate data from`sdb1` and `sdb2` to `ddb`.

First, write a config file:

```PYTHON
import os

# SQL folder location. Set as current working directory by default
ROOT_DIR = os.path.curdir

DATABASES = {
    'sources': [
        {
            'name': 'sdb1',
            'url': 'mysql://******'
        },
        {
            'name': 'sdb2',
            'url': 'mssql+pymssql://******',
            'use_view': True
        }
    ],
    'dest': {
        'name': 'ddb',
        'url': 'mysql://******'
    }
}

ORDERS = [
    'TABLE1',
    'TABLE2',
    'TABLE3',
]

INITIALS = [
    'INITIAL'
]
```

Then create some file structured under `ROOT_DIR` as below:

```bash
|--sdb1
    |-- TABLE3.SQL 
|--sdb2
    |-- TABLE1.SQL
    |-- TABLE2.SQL
|--ddb
    |-- INITIAL.sql
```

Finally, run this migration. The program will execute `TABLE1.SQL` in `sdb2` and write the result to `TABLE1` in `ddb`. For example:

```sql
SELECT filed1 AS d_filed1,filed2 AS d_filed2 FROM sdb2_table
```

This will migrate `filed1` to `d_filed1` and `filed2` to `d_filed2`. If you want to migrate the whole table to `TABLE1` of `ddb`, you can use this:

```sql
SELECT * FROM sdb2_table
```

Then the program will migrate other tables in the above way in the order set by `ORDERS`. After all migration has been finished, `INITIAL.SQL` is executed in `ddb`. 

With the `'use_view' : True` setting. we can reference migrated tables in subsequent SQL. In the above example, we can write this in `TABLE2.SQL`.

```sql
SELECT d_filed1 FROM TABLE1
```
## Dependencies

Python 2.7

- [sqlalchemy](https://www.sqlalchemy.org/)
- [pandas](http://pandas.pydata.org/)

## Contribute

This project only be tested in one situation: migration from SQL Server 2000 to MySQL 5.6.  Migration of other situations are under developing.

The library is very simple, only 100 lines of code. If you want to contribute, you can:

- fork
- write code
- pull request

If you have any question or advice, hope your issue.