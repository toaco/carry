# Carry ![](https://img.shields.io/badge/python-2.7,3.5,3.6-blue.svg) ![](https://badge.fury.io/py/carry.svg) ![](https://landscape.io/github/toaco/carry/master/landscape.svg?style=flat)

[中文文档](./docs/README_zh_CN.md)

# Feature

- Easy to use
- Support for data migration between commonly used relational databases and CSV files, including Firebird, Microsoft SQL Server, MySQL, Oracle, PostgreSQL, SQLite, Sybase
- ETL
    - Supports extracting data  from multiple tables simultaneously to one target table
    - Supports adding, deleting and complex conversion of extracted data, such as adding fields, deleting fields, modifying fields, adding rows, deleting rows, splitting rows, merging rows, cleaning data, desensitizing data, etc.
    - Support for referencing migrated tables when migrating new table (Only for relational databases, achieved by database view)

# Installation

```python
pip install carry
```

# Configuration

There is an example: Migrating data from an Oracle database to the `table_a` table in a MySQL database.

```python
# configure databases
STORES = [
    {
        'name': 'oracle_db',
        'url': 'oracle://username:password@host:port/dbname',
    },
    {
        'name': 'mysql_db',
        'url': 'mysql://username:password@host:port/dbname',
    }
]
# configure the ETL process
TASKS = [
    {
        'from': [{
            'name': 'oracle_db'
        }],
        'to': {
            'name': 'mysql_db',
        },
        'orders': [
            'table_a',
        ]
    }
]

if __name__ == '__main__':
    import carry
    carry.run (__ file__)
```

## STORES Configuration

`STORES` is used to configure the databases, a database can be a relational database or a CSV folder. The value of `STORES` is a list, each item of which is a dict and represents a database's configuration. The dict need a `name` key to be the identifier of the database.

For relational databases, you need to set the `url` key, which is the SQLAlchemy connection string for that database.And if you set `create_view: True`. Carry will create a `VIEW` for extracted data.

## TASKS Configuration

`TASKS` is used to configure the migration process. Its value is a list, each item in it represents an ETL process, and each ETL process is configured via a dictionary that contains ` from`, `to` and `orders` keys.

- `from` is used to configure the data source, its value is a list, each list contains a dictionary used to set the properties of the data source, the dictionary must have a `name` key to identify a database configured in` STORES`
- `to` is similar to` from`, which stand for the target database. The difference is that the value is a dictionary, not a list.
- `orders` configure subtasks. A sub-task can represent a table migration, a SQL script execution or a python code execution. 

## Sub-task Configuration

Carry currently has three types of subtasks: `Table Migration Task`, `SQL Script Task` and `Python Task`.

### Table Migration Task

A table migration task represents extracting data from data sources, transform it(optionally) and finally load it into a table in the target database.

Carry will decide what to be extracted from sources by the following steps:

1. If data source `A` is a relational database and `A` has a table named `table_a`, then all data of the table will be extracted

2. If data source `A` is a relational database and the `table_a.sql` file is found in the `./oracle_db`folder, Carry will execute `table_a.sql` in `A` to query data which will be extracted
3. If data source `A` is a CSV folder and the `table_a.csv` file is found in the `./oracle_db`folder. All data of the file will be extracted

If the table `table_a`  not existed in the target database, Carry will auto create it.

**NOTE:** Current version of the Carry will truncate `table_a` in the target database before migration. 

If you need to migrate the data of all tables in the data source `store_name` to the target database, you can use `store_name.* ` and if the data source is a relational database, Carry will migrate the tables according to the foreign key relationships between the tables. For example, Table A depends on Table B, then the migration of Table B will precede A.

#### Transform configuration

To transform the extracted data, we should define a `transform` function within  `Table Migration Task`. The function has `cursor` and `dest` parameters. 

```python
def transform_table_a(cursor, dest):
    while True:
        row = cursor.fetch()
        dest.insert(row)
        
...
    'orders': [
       ('table_a', transform_table_a),
    ]
...
```

### cursor

`cursor.fetch()` method return the next row of  the extrated data. The return value is a `Row` object and we can get or set its field via the `.`  or ` [] `,such as `row.ID` or` row[ID]`.  `row.copy()` method return a copy of the row.

`cursor.fetch()` method will throws a `NoResultFound` exception if there is no next row. we normally don't need to catch it and carry will start the next sub-task when catching it.

### dest

`dest` represents the target database and provides an `insert(row)` method, which inserts data from a `Row` object into the target table.

### SQL Script Task

The SQL Script task represents executing SQL script in the target database and is therefore only available when the target database is a relational database. The SQL script task is represented as a string ending with `.sql`. Carry will look for the script file in the`./mysql_db`folder and execute it in the `mysql_db` database.

```python
'orders': [
  'table_a',
  'insert.sql'
]
```

### Python Task

The Python task is a Python callable object (such as a function, method, or class that implements `__call__` method). The object can not have parameters. Such as:

```python
'orders': [
  'table_a',
   lambda: print 'hello'
]
```

# Contribution

I'm grateful to everyone to contribute bugfixes and improvements.