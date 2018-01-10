<p align="center">
    <img width="100" height="100" src="https://image.flaticon.com/icons/svg/239/239981.svg">
</p>

<p align="center">
    <img src="https://img.shields.io/badge/python-2.7-blue.svg">
    <img src="https://badge.fury.io/py/carry.svg">
    <img src="https://landscape.io/github/toaco/carry/master/landscape.svg?style=flat">
</p>

Carry是一个数据迁移工具，可以按照预先定义的规则提取数据，对提取的数据进行处理，最后再保存数据。下面是Carry的特性:

- 支持常用的关系型数据库以及CSV文件之间的数据迁移,包括:[Firebird](http://docs.sqlalchemy.org/en/latest/dialects/firebird.html),[Microsoft SQL Server](http://docs.sqlalchemy.org/en/latest/dialects/mssql.html), [MySQL](http://docs.sqlalchemy.org/en/latest/dialects/mysql.html),[Oracle](http://docs.sqlalchemy.org/en/latest/dialects/oracle.html), [PostgreSQL](http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html), [SQLite](http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html), [Sybase](http://docs.sqlalchemy.org/en/latest/dialects/sybase.html)


- Extract: 支持同时从多张表抽取多个字段的数据到目标表中
- Transform: 支持对抽取到的数据进行增加,删除以及复杂的转换操作.如: 增加字段,删除字段,修改字段,增加行,删除行, 行拆分,行合并,数据清洗,数据脱敏等
- Load: 支持在目标表不存在的情况下迁移数据.程序会自动创建字段名和字段类型正确的表
- 配置简单,使用方便: 只需要编写一个配置文件,提供`STORES`和`TASKS`配置即可, 同时提供了多种语法糖简化配置文件的编写
- 提供了包含对于剩余迁移时间的估计的进度条
- 高性能: 支持分块获取,分块处理以及分块保存数据
- 视图支持: 迁移数据时,支持引用已迁移的表(针对关系型数据库)

## 安装

```python
pip install carry
```

## 配置文件

使用Carry最重要的就是编写好配置文件,下面根据实例来描述如何编写配置文件.

先从一个最简单的例子开始:从一个oracle数据库迁移数据到mysql中的table_a表.

```python
# STORES 用于配置数据仓库
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
# TASK 用于配置ETL过程
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
    carry.run(__file__)
```

### STORES配置

`STORES`用于配置仓库，一个仓库可以是一个关系型数据库或者是一个CSV文件夹。`STORES`的值是一个列表，其每一项都表示一个仓库的配置，仓库必须设置`name`键的值，该值将用作仓库的标识符。

对于关系型数据库，需要设置url键，其值为该数据库的SQLAlchemy连接字符串。上面的例子中就表示配置了一个名为`oracle_db`和一个名为`mysql_db`的仓库. 

### TASKS配置

`TASKS`用于配置迁移过程,其值是一个列表,列表中的每一项都表示一个ETL过程，每个ETL过程都通过一个字典配置，该字典至少包含`from`,`to`和`orders`键。

- `from` 用来配置数据源，其值是一个列表，每个列表包含一个字典用于设置数据源的属性，该字典必须有`name`键(使用`STORES`中配置的的仓库名)，表示数据源的名称。
- `to` 类似于`from`，表示目标仓库. 区别是值是一个字典，而不是列表。
- `orders` 配置子任务。一个子任务可以表示一张表的迁移，也可以表示一个sql语句的执行等，关于子任务将在后面详细描述。

上面的例子中的`TASKS`配置表示: 从名为`oracle_db`的数据仓库提取数据到名为`mysql_db`的数据仓库中的`table_a`表中去. 至于提取哪些数据,如何对数据处理,以及对于目标仓库有何要求将在下节中的`表迁移任务`进行详细的描述.

## 子任务配置

Carry目前三种类型的子任务：`表迁移任务`，`SQL语句执行任务`和 `Python可调用对象调用任务`。Carry在迁移过程中将按照`orders`中定义的顺序依次执行子任务.

### 表迁移任务

表迁移任务表示从数据源中迁移数据到目标仓库中的一张表（如果目标仓库是CSV文件夹，则表示一个CSV文件）。表迁移任务直接使用目标数据库中的表明即可，如`table_a`.

Carry将按照该顺序决定从何处迁移哪些数据到目标表，以之前的配置为例:

1. 首先Carry在所有的数据源中寻找`table_a`的数据来源：
   1. 如果数据源A是关系型数据库，且A中有一张表名为`table_a`，则该表的数据的所有将被迁移到目标表中
   2. 如果数据源A是关系型数据库，且在程序当前目录下的`数据源A的名字/`目录下找到了`table_a.sql`文件，那么Carry将在数据源A中执行`table_a.sql`中的查询语句，并将查询结果迁移到目标表中
   3. 如果数据源A是CSV文件夹，且在程序当前目录下的`数据源A的名字/`目录下找到了`table_a.csv`文件，那么该文件的所有数据都将被迁移到目标表中
2. 对数据进行转换.(本例中未做数据转换, 该部分将在下节描述)
3. 将转换后的数据放入目标仓库
   1. 如果目标仓库是关系型数据库,但是`table_a`表不存在,则Carry会自动创建`table_a`表
   2. 如果目标仓库是CSV文件夹，且目标表不存在,则Carry会在当前目录下的`目标仓库的名字/`目录下创建`table_a.csv`文件

**另外需要注意的是:**当前版本的Carry会在迁移前清空目标仓库中的`table_a`表(如果存在). 该行为计划在之后的版本中改变.

---

#### 语法糖: `store_name.*`

如果需要将数据源`store_name`中的所有表的数据都迁移到目标仓库中，可以直接使用`store_name.*`的简写方式。Carry会自动迁移数据源`store_name`中的所有表，同时如果数据源是关系型数据库，Carry会根据表之间的外键关系迁移表。比如表A依赖于表B，那么表B的迁移将先于A迁移。

---

### SQL脚本任务

SQL脚本任务表示在目标仓库中执行SQL脚本，因此只有在目标仓库是关系型数据库时可以使用。SQL脚本任务用`.sql`结尾的字符串表示。Carry将在程序当前目录下的`目标仓库名/`文件夹下寻找该脚本文件并在目标仓库中执行。如在迁移完毕`table_a`之后,在`mysql_db`仓库中执行`当前目录/mysql_db/insert.sql`文件可以这样写:

```python
'orders': [
  'table_a',
  'insert.sql'
]
```

### Python可调用对象调用任务

Python可调用对象调用任务就是一个Python可调用的对象（比如函数，方法，或者实现了__call__的类），该调用对象不能有参数，Carry将直接调用该对象。如:

```python
'orders': [
  'table_a',
   lambda : do something ...
]
```

**调整计划**: 计划在之后的版本中在调用该对象时传递一些环境参数

## Transform配置

如需要对从数据源提取到的`table_a`的数据进行处理, 我们需要配置好`Transform`过程,`Transform`属于表迁移任务中的一部分,其值为一个Python可调用对象,该对象目前将接受`cursor`,`dest`两个参数.带有`Transform`的表迁移任务的格式为:`('表名', Python可调用对象)`,如:

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

该例子是不实用意义的,仅用于描述`Transform`过程. 其表示将获取到的`table_a`数据中的每一行放入目标仓库中去.

### cursor

cursor是一个游标,其`fetch`方法表示从获取到的数据中拿到下一行. 其返回值是一个`Row`对象.该对象可以通过`.`操作符或者`[]`符号访问其字段值,比如`row.ID`和`row['ID']`都可以用于访问该行中的`ID`字段.

cursor提供了`copy`方法用于拷贝自身,常用于行拆分的时候.

如果`fetch`没有拿到任何行,该方法将抛出`NoResultFound`异常.一般情况下不需要处理该异常,Carry捕捉到该异常后将自动开始下一个子任务.

### dest

dest表示目标仓库,目前提供了`insert(row)`方法,表示将一个`Row`对象中的数据插入到目标表中.

**注意:**对于通过`cursor.fetch`拿到的数据,如果不执行`dest.insert`,将造成该行的丢失.

## 配置详解

到目前为止,我们已经了解了Carry中最基础和常用的功能.接下来将对一些高级的配置进行讲解.

### STORES详解

Carry目前支持关系型数据库仓库和CSV文件夹仓库，每个仓库都必须设置`name`的值，同时还可以针对不同类型的仓库进行详细的配置。

针对关系型数据库：

- 可以设置`create_view`，当值为True时，Carry在执行`表迁移任务`的时候会将在数据库建立基于该查询的视图。同时可以设置`view_prefix`表示前缀。比如执行`table_a`迁移任务，执行完毕之后数据库中将会存在一个`table_a`视图。该视图的主要可以供后续的表迁移任务引用。
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

非常感谢你能够读到这里, 如果你是Python开发者并且对Carry感兴趣的话欢迎您加入进来一起完善`Carry`, 如果不是就star一下项目以示鼓励😁

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

未实现的(按照优先级排序)：

- 提供命令行程序


- 并发和并行


- 提供完整的CSV导入导出功能: 支持数据库本身的命令导出/导入数据，比如MySQL的`SELECT INTO FILE`和`LOAD FILE`


- 自动化测试, 性能优化,Python3支持...

