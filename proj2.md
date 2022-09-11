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



Page作为最低层，可以先行实现



**研究page类发现：几乎所有的对page的操作都有对应的封装函数，可以在这些封装函数中加上诸如错误处理之类的一些操作**

