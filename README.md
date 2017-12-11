Carry是一个数据迁移工具，可以按照预先定义的规则抽取数据，对抽取的数据进行处理，最后再保存数据。比如从一个数据库的两种表中提取一些数据，然后对每一行的数据生成一个ID，最后保存到另一个数据库中去。使用Carry只需要配置好`STORES`和`TASKS`，`STORES`代表仓库集合，`TASKS`用于设置迁移过程。

Carry使用`SQLAlchemy`链接数据库,因此理论上`SQLAlchemy`支持的数据库Carry都支持，包括：[Firebird](http://docs.sqlalchemy.org/en/latest/dialects/firebird.html)，[Microsoft SQL Server](http://docs.sqlalchemy.org/en/latest/dialects/mssql.html), [MySQL](http://docs.sqlalchemy.org/en/latest/dialects/mysql.html),[Oracle](http://docs.sqlalchemy.org/en/latest/dialects/oracle.html), [PostgreSQL](http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html), [SQLite](http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html),[Sybase](http://docs.sqlalchemy.org/en/latest/dialects/sybase.html)。另外，Carry还支持CSV文件的导入和导出。通过Carry，你可以轻易的完成：

1. 从数据库迁移数据到数据库
2. 从数据库迁移数据到CSV
3. 从CSV迁移数据到数据库
4. 从CSV迁移数据到CSV（目前需要通过结合2和3来实现）

下面使用Carry的一个模板：

```python
STORES = [
    {
        'name': '',
        'url': '',
    },
    {
        'name': '',
        'url': '',
    }
]
TASKS = [
    {
        'from': [{
            'name': ''
        }],
        'to': {
            'name': '',
        },
        'orders': []
    }
]
if __name__ == '__main__':
    import carry
    carry.run(__file__)
```

## 配置简介

### STORES配置

`STORES`配置仓库，一个仓库可以是一个关系型数据库或者是一个CSV文件夹。`STORES`的值是一个列表，列表每一项表示一个仓库的配置，仓库必须设置`name`键的值，该值将用作仓库的标识符。对于关系型数据库，需要设置url键，其值为该数据库的sqlalchemy连接字符串。下面的例子表示配置了一个MySQL仓库和一个Oracle仓库。

```python
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
```

### TASKS配置

`TASKS`是一个列表，列表中的每一项代表一次迁移过程，每个迁移过程通过一个字典配置，该字典至少包含`from`,`to`和`orders`键。

- `from` 用来配置数据源，其值是一个列表，每个列表包含一个字典用于设置数据源的属性，该字典至少需要有`name`键(使用`STORES`中配置的的仓库名)，表示数据源的名称。
- `to` 类似于`from`，区别是值是一个字典，而不是列表，该字典同样需要设置`name`的值。
- `orders` 配置子任务列表。一个子任务可以表示一张表的迁移，也可以表示一个sql语句的执行等，关于子任务将在后面详细描述。

下面是一个例子：

```python
TASKS = [
    {
        'from': [{
            'name': 'oracle_db'
        }],
        'to': {
            'name': 'mysql_db',
        },
        'orders': [
            'table1',
            'insert.sql',
            'table2'
        ]
    }
]
```

该配置表示从`oracle_db`迁移数据到`mysql_db`,迁移顺序为：先从`oracle_db`迁移数据到`mysql_db`中的`table1`表,然后在`mysql_db`中中执行`insert.sql`文件中的sql语句。（该文件需要在当前目录下的mysql_db文件夹中），最后迁移`table2`表。

##子任务详解

Carry目前三种类型的子任务：`表迁移任务`，`SQL语句执行任务`和`Python可调用对象调用任务`。

### 表迁移任务

表迁移任务表示从数据源中迁移数据到目标仓库中的一张表（如果目标仓库是CSV文件夹，则表示一个CSV文件）。表迁移任务直接使用目标数据库中的表明即可，如`table1`.

Carry将按照该顺序决定从何处迁移哪些数据到目标表，以`table1`为例子：

首先在所有的数据源中寻找`table1`的数据来源：

- 如果数据源A是关系型数据库，且A中有一张表名叫`table1`，则该表的数据的所有将被迁移到目标表中
- 如果数据源A是关系型数据库，且在程序当前目录下的`数据源A的名字/`目录下找到了`table1.sql`文件，那么Carry将在数据源A中执行`table1.sql`中的查询语句，将查询结果迁移到目标表中
- 如果数据源A是CSV文件夹，且在程序当前目录下的`数据源A的名字/`目录下找到了`table1.csv`文件，那么该文件的所有数据都将被迁移到目标表中

找到数据来源之后，Carry就可以将数据放入目标表。如果目标仓库是CSV文件夹，Carry会将数据放入该文件夹的`table1.sql`文件中。如果目标仓库是关系型数据库，则会插入数据到相应的表中。

另外如果需要将数据源`source_name`中的所有表的数据都迁移到目标仓库中，可以直接使用`source_name.*`的简写方式。Carry会自动迁移数据源`source_name`中的所有表，同时如果数据源是关系型数据库，Carry会根据表之间的外键关系迁移表。比如表A依赖于表B，那么表B的迁移将先于A迁移。

针对表迁移任务，Carry在迁移之前会将目标仓库中的改表中的所有数据清空。

### SQL脚本任务

SQL脚本任务表示在目标仓库中执行SQL脚本，因此只有在目标仓库是关系型数据库时可以使用。SQL脚本任务用`.sql`结尾的字符串表示。Carry将在程序当前目录下的`目标仓库名`文件夹下寻找该脚本文件并在目标仓库中执行。

### Python可调用对象调用任务

Python可调用对象调用任务就是一个Python可调用的对象（比如函数，方法，或者实现了__call__的类），该调用对象不能有参数，Carry将直接调用该对象。

## 配置详解

### STORES详解

Carry目前支持关系型数据库仓库和CSV文件夹仓库，每个仓库都必须设置`name`的值，同时还可以针对不同类型的仓库进行详细的配置。

针对关系型数据库：

- 可以设置`create_view`，当值为True时，Carry在执行`表迁移任务`的时候会将在数据库建立基于该查询的视图。同时可以设置`view_prefix`表示前缀。比如执行`table1`迁移任务，执行完毕之后数据库中将会存在一个`table1`视图。该视图的主要作用是可以供后续的表迁移任务使用。
- 可以设置`engine_config`，Carry使用`SQLAlchemy`操作数据库，该字典中的键值对将会传递给`SQLAlchemy`的`create_engine`方法。

### from详解

from用来配置数据来源，每个来源至少需要`name`值，此外可以针对不同类型的来源配置获取数据时的行为。

针对关系型数据库：
- 设置`chunksize`，如果该键的值为`None`，那么表示从数据源获取所有数据，然后迁移到目标表中。如果设置为数字a，则表示每次从数据源获取a条记录，然后放入目标表中之后再继续获取a条记录，直到完成迁移。该键的默认值为`10000`。

### to详解

from用来配置目标仓库，每个目标仓库至少需要`name`值，此外可以针对不同类型的目标仓库配置放置数据时的行为。

针对关系型数据库：
- 设置`chunksize`，如果该键的值为`None`，那么表示一次性放入所有数据。如果设置为数字a，则表示每次放入a条数据直到完成迁移。该键的默认值为`10000`。
- 设置`if_exists`，如果该键值为`replace`，表示如果该表已经存在则会被删除后重建，如果不存在则会创建。如果为`append`，则表示表存在的时候直接将数据追加到表中，表不存在则创建。默认值为`append`

针对CSV文件夹：
- 设置行分隔符，字段分隔符，文件编码等内容。

所有的from和to的默认值都可以在`carry/defaults.py`中查看。

## 开发进度

Carry的一些功能还没有实现完整，包括：

- `use_view` 目前仅支持`SQL Server`和`Oracle`
- 从CSV导入数据到关系型数据库。目前仅支持导入数据到MySQL，且使用的方式为`LOAD FILE`

已经测试过的迁移过程：

| 源               | 目标            |
| --------------- | ------------- |
| SQL Server 2008 | MySQL 5.6/CSV |
| Oracle 9i       | MySQL 5.6/CSV |
| MySQL 5.6       | MySQL 5.6/CSV |
| CSV             | MySQL 5.6     |

未实现的：

- Carry是一个ETL工具（Extract-Transform-Load），目前还没有实现Transform过程。比如为从数据源获取的每一行数据增加一个ID字段且设置值
- 支持使用数据库本身的命令导出/导入数据，比如MySQL的`SELECT INTO FILE`和`LOAD FILE`。该方式主要是速度非常快
- 提供命令行程序


- 多线程/多进程支持，同时需要协调各个任务之间的次序
- 使用TravisCI完成自动化测试
- 更多的异常处理
- 针对不同的迁移方式进行性能优化
- Python3支持
- ...

