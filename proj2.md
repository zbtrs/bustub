# Proj2

先要实现两个page

## Directory Page

Directory保存在单独的一个page之中，用一个类来实现，这个类中包含的成员变量：

page_id_: 描述当前page的page_id

lsn_:以后用到的

global_depth_

local_depths_:是一个数组，保存每个bucket的local depth

bucket_page_ids_:保存每个bucket的page_id_t，是一个数组，第i个元素是第i个bucket的page_id

## Bucket Page

三个数组：

occupied_:第i位如果是1则表示array_的第i位 被占用了

readable_:第i位是1则表示array_的第i位是一个可以读取的值

array_:用来存key_value pairs

每个hash table中存的都是定长数据类型，在一个单独的hash table实例中，所有存储的key/values的长度都是相等的。



**每个bucket page和directory page都是从Buffer pool中取出来的page中的内容，每次要去读或写这个page，都要先从buffer pool中用page_id来fetch，然后用reinterpret_cast来进行类型转换，在读写操作后unpin这个page**

```c++
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());
```

```c++
  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());
```

Page作为最低层，可以先行实现



**研究page类发现：几乎所有的对page的操作都有对应的封装函数，可以在这些封装函数中加上诸如错误处理之类的一些操作**



page中实现的函数只需要处理好管理page本身，（相当于只需要做好保管内存上的信息）。至于如何调用，调用的策略，都交给上层的hash_table来处理



这里bucket page要实现自增长，一开始桶只能容纳一堆key-value，一旦在一个位置放了一个key-value，那么就认为这个位置是occupied了，以后就算delete了也是occupied。如果一个位置的key-value被删除了，那么这个位置就是不可读的。

在一个hash table的Page中再来用一个map感觉是一个非常蠢的行为，查找的话感觉只能线性查找？以后再来优化



## Extendible Hash Table

### 成员变量

directory_page_id_: 类型为page_id_t，在初始化的时候得知

buffer_pool_manager_: BufferPoolManager*，初始化的时候已经拿到了，不需要自行处理，注意这是一个指针类型

comparator_: 是一个KeyComparator，也不需要经过特殊处理，是一个function类



### 构造函数

只需要处理directory_page_id_: 

```c++
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());
```

```c++
  directory_page->IncrGlobalDepth();
```

update:

因为一开始GlobalDepth设置为1，所以要新建两个BucketPage,一个对应0，一个对应1，并且把这两个BucketPage的localDepth设置为1，调用对应的helper function

### 析构函数



### helper function

#### Hash(KeyType key) -> uint32_t

直接把key传进去，返回一个32位的值，注意不能直接用这个hash值，要跟globaldepth表示的二进制数and一下。

无需我们自己实现。

#### KeyToPageId

传入两个参数key和*dir_page，在外层先把Hash and后的结果算出来，然后用dir_page里面的GetBucketPageId来得到page_id

#### FetchDirectoryPage

根据成员变量中的directory_page_id来从bufferpool中取得这个directory page

要做类型转换

一旦调用了这个函数，必须要在后面加上Unpin操作

#### FetchBucketPage

和上面差不多，要类型转换和Unpin

#### SplitBucketPage 

1.传入的是一个指向BucketPage的指针和对应的local_depth，还有这个bucketpage的page_id，返回的是一个新分裂的page的pageid。(记得Unpin掉新分裂出来的page)

2.++local_depth。新建一个page，设置新的page的local_depth。

3.在bucket_page中写一个helper function，将所有位置上的key-value存到一个vector中。这个helper function接受一个指向vector的指针。

4.bucket_page调用clear函数，然后将这些元素的key hash之后and上GLOBAL_MASK，分别insert到两个bucket page中。

5.Unpin掉新创建的page。

#### UpdateDirectoryPage 

1.在调用这个函数的时候,globaldepth已经++了。

2.先处理因为globaldepth++而多出来的目录位，方法是只看globaldepth-1位指向的位置。（这里可以直接把上半部分的指针复制下来）

3.接下来进行调整：遍历整个directorypage，对于指向分裂的bucketpage的位置，看最高位是不是1，如果是1，就更改指向的对象。如果更改了指向的对象，就一定要更改localdepth！！！

### Insert 

1.先FetchDirectoryPage(注意最后一定要Unpin!!!)

2.调用KeyToPageid，然后调用FetchBucketPage，得到即将要插入的bucket page

3.调用KeyToDirectoryIndex，看在directorypage中的这一页的localsize，和bucket page里面的size_对比一下看看是不是满了

4.如果没有满，则直接调用bucket page的insert函数插入即可。这里如果返回了false就直接返回false。

5.如果满了，并且localdepth < globaldepth，写一个帮助函数SplitBucketPage。

6.如果满了，并且localdepth == globaldepth，++globaldepth，并且从0到GLOBAL_MASK遍历一遍directory，首先把指向要分裂的bucketpage的index给更新了，然后把新加入directory中的index也给更新一下。

7.Unpin directorypage和bucketpage。



一个问题：新创建的bucket的localdepth要设置，但是不是在bucket page里面设置，而是要在directory page里面设置？应该把这个任务交给updatedirectorypage函数



update：如果没有更新globaldepth，那么更新directorypage就要用另外一种方法。



## Merge --TODO

传入directory_page的指针，两个要合并的桶的id0,id1，这里统一把id1合并到id0上面去。

扫描directory_page，将指向id0和id1的localdepth--，然后更改指针。



## Remove --TODO

1.先FetchDirectoryPage,然后拿到BucketPage(类似Insert)

2.如果在bucketpage中找不到key-value，Unpin然后返回。

3.如果bucketpage删除了这个key-value后不为空，直接返回。

4.如果变成空桶了，看另外一个兄弟桶是否为空。如果都为空，则调用Merge函数。要更新localdepth。

5.看所有的local_depth是否都小于global_depth了，分两种情况更新directorypage。（这里是否要用一个数据结构来维护？）

6.最后Unpin。





