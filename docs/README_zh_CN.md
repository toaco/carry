# Carry ![](https://img.shields.io/badge/python-2.7,3.5,3.6-blue.svg) ![](https://badge.fury.io/py/carry.svg) [![Build Status](https://travis-ci.org/toaco/carry.svg?branch=master)](https://travis-ci.org/toaco/carry) ![](https://landscape.io/github/toaco/carry/master/landscape.svg?style=flat) [![Coverage Status](https://coveralls.io/repos/github/toaco/carry/badge.svg?branch=master)](https://coveralls.io/github/toaco/carry?branch=master)


Carry是一个基于SQLAlchemy和Pandas实现的数据迁移工具.

## 特性

- 配置简单,易于使用


- ETL过程
  - 支持常用的关系型数据库以及CSV文件之间的数据迁移,包括:Firebird, Microsoft SQL Server, MySQL, Oracle, PostgreSQL, SQLite, Sybase
  - 支持迁移使用SQL语句查询出来的数据,并可根据该SQL语句自动创建视图,供后续的迁移任务引用
  - 支持对抽取到的数据进行复杂的转换.如: 增/删/改字段,增/删/改行, 拆分行,合并行等
- 性能
  - 实现了生产者多消费者模式,加快单表的迁移速度
  - 实现了多表的并行迁移

## 设计

![structure](structure.png)

## 安装

```python
pip install carry
```

## 基本配置

使用Carry只需要提供迁移过程的配置,下面根据一个实例来描述如何编写配置文件: 从一个oracle数据库迁移数据到mysql中的table_a表.

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
```

### 启动方式

Carry有两种执行方式,第一种是将配置保存成一个py文件,然后在文件的目录中执行`carry 文件名`命令.如果文件名为`carfile.py`,则可以省略文件名部分,执行`carry`即可.

第二种方式是调用carry的run方法,需要将配置文件的路径传递给该方法.如:

```python
if __name__ == '__main__':
    import carry
    carry.run (__ file__)
```

### STORES配置

`STORES`用于配置仓库，一个仓库可以是一个关系型数据库或者是一个CSV文件夹。`STORES`的值是一个列表，其每一项都表示一个仓库的配置，仓库必须设置`name`键的值，该值将作为仓库的标识符。

对于关系型数据库，需要设置url键，其值为该数据库的SQLAlchemy连接字符串。上面的例子中配置了一个名为`oracle_db`和一个名为`mysql_db`的仓库. 

### TASKS配置

`TASKS`用于配置迁移过程,其值是一个列表,列表中的每一项都表示一个ETL过程，每个ETL过程都通过一个字典配置，该字典需要设置`from`,`to`和`orders`的值。

- `from` 用来配置数据源，其值是一个字典列表，每一个字典需要设置`name`值(使用`STORES`中配置的的仓库名)，表示数据源的名称。
- `to` 类似于`from`，表示目标仓库. 区别是值是一个字典，而不是字典列表。
- `orders` 配置子任务。一个子任务可以是一张表的迁移，也可以是一个sql语句的执行等，关于子任务将在后面详细描述。

上面的例子中的`TASKS`配置表示: 从名为`oracle_db`的数据仓库提取数据到名为`mysql_db`的数据仓库中的`table_a`表中去. 

接下来将描述Carry如何知道提取哪些数据,如何对数据进行处理.

## 子任务配置

Carry目前三种类型的子任务：`表迁移任务`，`SQL脚本任务`和 `Python任务`。

表迁移任务表示从数据源中迁移数据到目标仓库中的一张表。SQL脚本任务表示在目标仓库中执行一些SQL脚本.Python任务就是调用一个Python对象,如函数.

### 表迁移任务

表迁移任务表示从数据源中迁移数据到目标仓库中的一张表。直接使用表名即可配置表迁移任务`table_a`.

Carry将按照该顺序决定从何处迁移哪些数据到目标表，以之前的配置为例:

1. 首先Carry在所有的数据源中寻找`table_a`的数据来源：
   1. 如果数据源A是关系型数据库，且在程序当前目录下的`数据源A的名字/`目录下找到了`table_a.sql`文件，那么Carry将在数据源A中执行`table_a.sql`中的查询语句，将结果用于迁移
   2. 如果数据源A是关系型数据库，且A中有一张表名为`table_a`，则该表的所有数据都将被迁移
   3. 如果数据源A是CSV文件夹，且在程序当前目录下的`数据源A的名字/`目录下找到了`table_a.csv`文件，则该表的所有数据都将被迁移
2. 对数据进行转换.(本例中未做数据转换, 该部分将在之后描述)
3. 将转换后的数据放入目标仓库
   1. 如果目标仓库是关系型数据库,但是`table_a`表不存在,则Carry会创建`table_a`表.如果表已经存在,Carry会清掉原始数据之后插入新数据
   2. 如果目标仓库是CSV文件夹，且目标表不存在,则Carry会在当前目录下的`目标仓库的名字/`目录下创建`table_a.csv`文件,否则清空原来的文件中的内容后插入新数据

如果需要将数据源`store_name`中的所有表的数据都迁移到目标仓库中，可以使用`store_name.*`配置。Carry会迁移数据源`store_name`中的所有表到目标仓库中.

#### table类

如果需要对表迁移任务进行更多的控制,则需要使用`table`类,其初始化函数如下:

```
__init__(self, name, transformer=None, header=None, get_config=None, put_config=None,
         dependency=None, source_name=None, effects=None)
```

- name: 目标表
- transformer: 数据转换函数
- header: 如果值为列表,表示只迁移这些列.如果值为字典,其键表示迁移的列,值表示该列的新列名
- get_config: 从数据源提取数据时的配置
- put_config: 向目标仓库插入数据的配置
- dependency: 该任务依赖的任务,其值为一个列表,Carry在执行了所有依赖的任务之后开始该任务.如果不设置,Carry会自动根据目标仓库中该表的外键关系生成依赖.
- source_name: 表示数据源中的表名(或者SQL文件名),如果不设置,其值等于name.
- effects: 该任务会影响到的目标仓库中的表.其值为列表,默认包含本次迁移的目标表. effects中定义的表,会在该任务迁移之前被清空.

#### transformer函数

transformer用于配置数据转换函数,其值为一个Python可调用对象,该对象需要接受`cursor`,和`dest`这两个参数.如:

```python
def transform_table_a(cursor, dest):
    while True:
        row = cursor.fetch()
        dest.insert(row)
```

#### cursor

cursor是一个游标,其`fetch`方法表示从获取到的数据中拿到下一行. 其返回值是一个`Row`对象.如果`fetch`没有拿到任何行,该方法将抛出`NoResultFound`异常. Carry捕捉到该异常后将开始下一个子任务.cursor也是一个可迭代对象,每次迭代返回下一个`Row`对象.

`Row`对象可以通过`.`操作符或者`[]`符号访问其字段值,比如`row.ID`和`row['ID']`都可以用于访问或者修改该行中的`ID`字段.如`row.ID = 1`.

cursor对象的其它方法:

- 使用`copy()`:方法拷贝自身,返回一个新的对象. 
- 使用`del` 关键字删除行的某个字段,如`del row['ID']`或者`del row.ID`

#### dest

dest表示目标仓库,通过其`insert(row)`方法可以将一个`Row`对象所表示的行插入到目标表中.

### SQL脚本任务

SQL脚本任务表示在目标仓库中执行SQL脚本，因此只有在目标仓库是关系型数据库时可以使用。SQL脚本任务用`.sql`结尾的字符串表示。Carry将在程序当前目录下的`目标仓库名/`文件夹下寻找该脚本文件并在目标仓库中执行。如:

```python
'orders': [
  'insert.sql'
]
```

同样,如果需要复杂的控制,则需要使用`sql`类,其初始化函数签名如下:

```
__init__(self, name, dependency=None, effects=None)
```

其中关键字参数的含义同`table`类.

### Python任务

Python任务就是提供一个Python可调用的对象（比如函数，方法，或者实现了__call__的类），该调用对象不能有参数，Carry将直接调用该对象。如:

```python
'orders': [
   lambda : do something ...
]
```

Carry同样提供了`py`类,其初始化函数签名如下:

```
__init__(self, callable_, dependency=None, effects=None)
```

`callable_`表示该Python可调用对象,其余关键字参数的含义同`table`类.

## 高级配置

到目前为止,我们已经了解了Carry中最基础和常用的功能.接下来将对一些高级的配置进行讲解.

### STORES

Carry目前支持关系型数据库仓库和CSV文件夹仓库，每个仓库都必须设置`name`的值，同时还可以针对不同类型的仓库进行详细的配置。

针对关系型数据库：

- 可以设置`create_view`，当值为True时，Carry在执行`表迁移任务`的时候会将在数据库建立基于该查询的视图。同时可以设置`view_prefix`表示前缀。比如执行`table_a`迁移任务，执行完毕之后数据库中将会存在一个`table_a`视图。该视图的主要可以供后续的表迁移任务引用。
- 可以设置`engine_config`，Carry使用`SQLAlchemy`操作数据库，该字典中的键值对将会传递给`SQLAlchemy`的`create_engine`方法。

### from

from用来配置数据来源，每个来源至少需要`name`值，此外可以针对不同类型的来源配置获取数据时的行为。

针对关系型数据库：
- 设置`chunksize`，如果该键的值为`None`，那么表示从数据源获取所有数据，然后迁移到目标表中。如果设置为数字a，则表示每次从数据源获取a条记录，然后放入目标表中之后再继续获取a条记录，直到完成迁移。该键的默认值为`10000`。

### to

from用来配置目标仓库，每个目标仓库至少需要`name`值，此外可以针对不同类型的目标仓库配置放置数据时的行为。

针对关系型数据库：
- 设置`chunksize`，如果该键的值为`None`，那么表示一次性放入所有数据。如果设置为数字a，则表示每次放入a条数据直到完成迁移。该键的默认值为`10000`。
- 设置`if_exists`，如果该键值为`replace`，表示如果该表已经存在则会被删除后重建，如果不存在则会创建。如果为`append`，则表示表存在的时候直接将数据追加到表中，表不存在则创建。默认值为`append`

针对CSV文件夹：
- 设置行分隔符，字段分隔符，文件编码等内容。

所有的from和to的默认值都可以在`carry/defaults.py`中查看。
