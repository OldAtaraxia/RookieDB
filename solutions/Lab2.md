## UnderStanding the Skeleton Code

太棒了, 还有这种部分.

### DataBox

用来代表各种数据类型.

data box中含有这几种数据类型: Boolean(1字节), Int(4字节), Float(4字节), Long(8字节), String(不定)

project中需要使用`Comparable<DataBox>`, ![image-20220322191150133](C:\Users\Lenovo\AppData\Roaming\Typora\typora-user-images\image-20220322191150133.png)

> Comprable接口 -> 只有一个方法`CompareTo`

### RecordId

表中的一条记录是由它的页号（它所在页的编号）和它的条目号（该记录在该页的索引）唯一标识的。这两个数字（pageNum，entryNum）构成了一个RecordId。

在这个项目中，我们将在我们的叶子节点中使用Record ID作为数据页中记录的指针。

### Index

这里提到的"Alternative 2 B+ Tree"指的是叶子节点中含有指向对应record的指针:

![image-20220322192158122](https://gitee.com/oldataraxia/pic-bad/raw/master/img/image-20220322192158122.png)

代码结构:

* `BPlusTree.java`: 管理B+树结构. 每一个B+树节点内部吧`DataBox`映射到之前提到的`RecordId`.  实现`get`, `put`之类的方法

```java
DataBox key = new IneDataBox(42);
RecordID rid = new RecordId(0, (short)0);

tree.put(key, rid);
tree.get(key);
tree.get(new IntDataBox(100));
```

* `BPlusNode.java`:  B+树的一个节点, 有类似BPlusTree的方法, 如get. put, delete等. 是一个抽象类, 实现类有`LeafNode`和`InnerNode`
  * `LeafNode` -> 没有子节点. 节点其中包含key-value对, 其中key是`Databox`value是`RecordId`. 以及指向右节点的指针.
  * `InnerNode` -> 存储子节点的key和指向其的指针. 
* `BPlusTreeMetadata.java` -> 存储树的一些元信息, 比如树的order数, 高度, 可以通过`this.metadata`来访问

注意事项:

* 这里实现的B+树不支持重复的主键, 遇到的时候抛出异常
* 这里假设了内部节点和叶子结点可以被序列化到同一个page上. 不需要考虑扩多个page的情况
* 删除操作时不需要rebalance整个树. 那这就是

[B树和B+树的插入、删除图文详解 - nullzx - 博客园 (cnblogs.com)](https://www.cnblogs.com/nullzx/p/8729425.html)

### LockContext objects

"锁的上下文对象"

在需要这个参数类型的方法时, 传入`this.lockContext`for`BPlusTree`, `this.treeContext`for`InnerNode`和`leafNode`

### `Optional<T> objects`

 we use `Optional`s for values that may not necessarily be present. For example, a call to `get` may not yield any value for a key that doesn't correspond to a record, in which case an `Optional.empty()` would be returned.  If the key did correspond to a record, a populated `Optional.of(RecordId(pageNum, entryNum))` would be returned instead.

### Project Structure Diagram

需要实现的方法与对应的提示图.

![img](https://3248067225-files.gitbook.io/~/files/v0/b/gitbook-legacy-files/o/assets%2F-MFVQnrLlCBowpNWJo1E%2Fsync%2Fb5164abe18f4d8a7a0174ce8119da13706554570.jpg?generation=1599942738303255&alt=media)

## Your Task

* LeafNode::fromBytes: 从page里读取一个`LeafNode`. 
  * 关于`LeafNode`的序列化过程参考`LeafNode::toBytes`
  * 关于如何读取参考`InnerNode::fromBytes`, 注意两种node的序列化差异
  * 通过`TestLeafNode::testToAndFromBytes`
* 实现`LeafNode`, `InnerNode`, and `BPlusTree`的`get`, `getLeftmostLeaf`(BPlusTree没有), `put`, `remove`
  * 这三种方法可以被看作递归, BPlusTree开始调用, InnerNode递归调用, 最后在LeafNode结束.
  * `LeafNode`和`InnerNode`中有`sync()`方法, 来确保缓冲区中的节点表示与内存中的是同步的. 实现`put`和`reomve`是要调用. 

> 话说这里的`sync`就是说对节点产生改变后要马上写回盘吗, 这也太笨拙了....

* Scan: 实现`BPlusTree`中的`scanAll`和`scanGreaterEqual`方法.
  * 为了实现它需要先实现`BPlusTreeIterator`内部类.
  * 不需要考虑在扫描过程中对树的修改. 
  * 通过`TestBPlusTree::testRandomPuts`
* Bulk Load批量加载: 在 `LeafNode`, `InnerNode`, and `BPlusTree`中实现`bulkLoad`. 
  * 通过project2所有的test

做完之后可以通过运行`CommandLineInterface.java`运行, 可以为表创建索引了:

![image-20220322224049618](https://gitee.com/oldataraxia/pic-bad/raw/master/img/image-20220322224049618.png)

测试样例不全, 比如从BPlusTree中删除所有东西后`hasNext`应该立刻返回false.

自己增加测试: 在`src/test/java/edu/berkeley/cs186/database/index`中加入新方法并用`@Test`注解标识

```java
@Test
public void testEverythingDeleted() {
    // your test code here
}
```

---

实现:

## exercise 1

`InnerNode::fromBytes`的实现:

* 首先从BufferManager获得Page对象, 并获得其Buffer对象. 之后得到其nodeType
* 创建`List<DataBox> keys`和`List<Long> children`(即节点内部保存的kv数组). 在读取一个`n`, 读取`n`次key, 读取`n`次children

```java
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                     LockContext treeContext, long pageNum) {
        // TODO(proj2): implement
        // Note: LeafNode has two constructors. To implement fromBytes be sure to
        // use the constructor that reuses an existing page instead of fetching a
        // brand new one.

        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();

        byte nodeType = buf.get();
        assert(nodeType == (byte)1);

        long rs = buf.getLong();
        Optional<Long> rightSibling = rs == -1 ? Optional.empty() : Optional.of(rs);
        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();

        int n = buf.getInt();

        for (int i = 0; i < n; i++) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
            rids.add(RecordId.fromBytes(buf));
        }

        return new LeafNode(metadata, bufferManager, page, keys, rids, rightSibling, treeContext);
    }
```

LeafNode的序列化方法在`toBytes`方法中显明了. 

![image-20220322231920879](https://gitee.com/oldataraxia/pic-bad/raw/master/img/image-20220322231920879.png)

因此最后的实现:

```java
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                     LockContext treeContext, long pageNum) {
        // TODO(proj2): implement
        // Note: LeafNode has two constructors. To implement fromBytes be sure to
        // use the constructor that reuses an existing page instead of fetching a
        // brand new one.

        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();

        byte nodeType = buf.get();
        assert(nodeType == (byte)1);

        Long rightSibling = buf.getLong();
        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();

        int n = buf.getInt();

        for (int i = 0; i < n; i++) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
            rids.add(RecordId.fromBytes(buf));
        }

        return new LeafNode(metadata, bufferManager, page, keys, rids, Optional.of(rightSibling), treeContext);
    }
```

---

## exercise 2

> 我觉着应该先整体地看一下BPlusNode的实现:
>
> * `LeafNode get(DataBox key)`: 返回在以当前节点为根节点的子树中key**可能所在的叶子节点**. 不管叶子节点中是否真的存在对应的值
> * `LeafNode getLeftmostLeaf()`: 回在以当前节点为根节点的子树中最左边的叶子节点
> * `Optional<Pair<DataBox, Long>> put(DataBox key, REcordId rid)`: 把(key rid)插入当前子树.  有两种情况:
>   * 若插入没有导致当前节点溢出, 则返回`Optional.empty()`
>   * 若插入导致了当前节点溢出, 把当前节点分裂成左右d和d+1两个节点并返回`(split_key, right_node_page_num)`. 其中`right_node_page_num`是新创建的右节点, `split_key`是用于提供给父节点作为新的key值的, 故其取决于当前节点是内部节点还是叶子节点:
>     * 叶子节点因为是分裂成了`d`和`d+1`两个节点, 其`split_key`要返回右节点的第一个key, 它要被插入父节点
>     * 内部节点则是分为两个`d`节点, 把中间节点作为`split_key`传送上去.
> * remove: 

---

Get

BPlusTree的实现: 递归调用root节点的`get`方法得到一个leafnode, 调用叶子节点的`getKey`方法. 

```java
public Optional<RecordId> get(DataBox key) {
    typecheck(key);
    // TODO(proj4_integration): Update the following line
    LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

    // TODO(proj2): implement
    // 调用root节点的get方法
    return this.root.get(key).getKey(key); // 这里的getKey已经有判空逻辑了
}
```

`InnerNode`有两个泛型方法: `numLessThanEqual(T x, L;;;;;;;;;;;;;;ist<T>ys)`和`numLessThan(T x, List<T> ys)`. 返回在`ys`中小于等于/小于`x`的元素数量(也就是大于/大于等于的第一个元素的index). 这两个工具函数是用来决定去到哪个子节点的.

注意这里子节点的数量 = 父节点key数量 + 1; keys中第index个key, 大于等于它的元素的子节点应该在children中排名index+1.

> InnerNode的children是一个`Long`类型的数组, 存的是pageNum. 真正的叶子节点用`getChild`工具函数来获得. 我猜是为了提高检索性能吧...(后来意识到更多的更重要的原因是因为并非所有的Node都是在内存里的. 还可能在磁盘上没有实例化..)
>
> InnerNode的keys数组才是子节点的key, 对应子节点的pageNum就是children数组. 每个节点都要实现toBytes

```java
@Override
public LeafNode get(DataBox key) {
    // TODO(proj2): implement
    int index = numLessThanEqual(key, this.keys);
    return this.getChild(index).get(key);
}
```

`LeafNode`: 返回自己:

```java
@Override
public LeafNode get(DataBox key) {
    // TODO(proj2): implement
    return this;
}
```

> 这里的判空策略具体是在顶层`BPlusTree`调用的`getKey`中实现的. LeafNode不需要判空只需要返回自己就行了

---

`getLeftMostLeaf()`: 

innerNode:

```java
@Override
public LeafNode getLeftmostLeaf() {
    assert(children.size() > 0);
    return getChild(0).getLeftmostLeaf();
}
```

leafNode:

```java
@Override
public LeafNode getLeftmostLeaf() {
    // TODO(proj2): implement
    return this;
}
```

---

put

这里因为涉及到修改需要用到`sync()`函数, 作用是把叶子节点写入page中.

从叶子节点开始实现:

```java
// See BPlusNode.put.
@Override
public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
    // TODO(proj2): implement
    if (keys.contains(key)) {
        throw new BPlusTreeException("duplicate keys");
    }

    int index = InnerNode.numLessThanEqual(key, keys); // 第一个大于key的地方, 之前的元素都小于key
    keys.add(index, key);
    rids.add(index, rid);

    // 判断是否溢出, 是则split
    if (keys.size() > 2 * this.metadata.getOrder() ) {
        return split();
    } else {
        sync();
        return Optional.empty();
    }
}

private Optional<Pair<DataBox, Long>> split() {
    assert (keys.size() == metadata.getOrder() * 2);
    assert (rids.size() == metadata.getOrder() * 2);

    List<DataBox> rightKeys = keys.subList(metadata.getOrder(), keys.size());
    List<RecordId> rightRids = rids.subList(metadata.getOrder(), rids.size());
    LeafNode rightNode = new LeafNode(metadata, bufferManager, rightKeys, rightRids, rightSibling, treeContext); // 创建一个新leafNode
    keys = keys.subList(0, metadata.getOrder());
    rids = rids.subList(0, metadata.getOrder());
    rightSibling = Optional.of(rightNode.getPage().getPageNum());

    sync();

    return Optional.of(new Pair(rightKeys.get(0), rightNode.getPage().getPageNum()));
}
```

内部节点:

```java
@Override
public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
    // TODO(proj2): implement
    int index = numLessThanEqual(key, keys);
    Optional<Pair<DataBox, Long>> putPair = getChild(index).put(key, rid);

    if (putPair.isPresent()) {
        keys.add(index, putPair.get().getFirst());
        children.add(index + 1, putPair.get().getSecond());
        // 判断是否溢出
        if (keys.size() > 2 * metadata.getOrder()) {
            return split();
        } else {
            sync();
            return Optional.empty();
        }
    } else {
        return Optional.empty();
    }
}

private Optional<Pair<DataBox, Long>> split() {
    assert(keys.size() == 2 * metadata.getOrder() + 1);
    assert(children.size() == 2 * metadata.getOrder() + 2);

    List<DataBox> rightKeys = keys.subList(metadata.getOrder() + 1, keys.size());
    List<Long> rightChildren = children.subList(metadata.getOrder() + 1, children.size());
    DataBox splitKey = keys.get(metadata.getOrder());

    InnerNode rightNode = new InnerNode(metadata, bufferManager, rightKeys, rightChildren, treeContext);
    keys = keys.subList(0, metadata.getOrder());
    children = children.subList(0, metadata.getOrder() + 1);
    sync();

    return Optional.of(new Pair(splitKey, rightNode.getPage().getPageNum()));
}
```



BPlusTree:

```java
public void put(DataBox key, RecordId rid) {
    typecheck(key);
    // TODO(proj4_integration): Update the following line
    LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

    // TODO(proj2): implement
    // Note: You should NOT update the root variable directly.
    // Use the provided updateRoot() helper method to change
    // the tree's root if the old root splits.
    Optional<Pair<DataBox, Long>> putPair = root.put(key, rid);
    if (putPair.isPresent()) {
        spiltRoot(putPair.get().getFirst(), putPair.get().getSecond());
    }
    return;
}

private void spiltRoot(DataBox key, Long child) {
    // 手动创建新的根节点
    List<DataBox> keys = new ArrayList<>();
    keys.add(key);
    List<Long> children = new ArrayList<>();
    children.add(root.getPage().getPageNum()); // 左孩子即原来的root
    children.add(child); // 右孩子即返回者
    BPlusNode newRoot = new InnerNode(metadata, bufferManager, keys, children, lockContext);

    updateRoot(newRoot);
}
```

---

remove:

不需要rebalance整棵树就就很简单了:

LeafNode:

```java
// See BPlusNode.remove.
@Override
public void remove(DataBox key) {
    // TODO(proj2): implement
    if (keys.contains(key)) {
        int index = keys.indexOf(key);
        keys.remove(index);
        rids.remove(index);
        sync();
    }
}
```

InnerNode: 

```java
// See BPlusNode.remove.
@Override
public void remove(DataBox key) {
    // TODO(proj2): implement
    int index = numLessThanEqual(key, keys);
    getChild(index).remove(key);
}
```

BPlusTree: 

```java
public void remove(DataBox key) {
    typecheck(key);
    // TODO(proj4_integration): Update the following line
    LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

    // TODO(proj2): implement
    root.remove(key);
}
```

## exercise 3

意识到BPlusIterator需要实现一个新的构造器来指定初始元素在初始节点中的位置, 而不是默认的第一个元素.

这里遇到了一个`pageNum == -1`导致报错没有找到页的问题. 排查得知是`LeafNode`在序列化是, 若`rightSilbing`是`Optional.empty()`, 会序列化为`-1`. 但我实现读的时候没有进行转化而是直接存了一个`-1`

```java
public Iterator<RecordId> scanAll() {
    // TODO(proj4_integration): Update the following line
    LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

    // TODO(proj2): Return a BPlusTreeIterator.
    return new BPlusTreeIterator(root.getLeftmostLeaf());
}
```

```java
    public Iterator<RecordId> scanGreaterEqual(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        // TODO(proj2): Return a BPlusTreeIterator.
        LeafNode startNode = root.get(key);
        return new BPlusTreeIterator(startNode, key);
    }
```

## exercise 4

草了, 本来以为很简单但是还很坑的. 

`LeafNode`中连续不断地给当前节点加元素, 直到满(溢出). 返回分裂出去的新节点. 返回后就意味着**要么数据没了要么当前节点分裂了、新的数据要被插入分类出去的新节点了(当前节点以后用不到了)**

`InnerNode`循环往最右边的节点加数据. 同理溢出就说明当前节点以后也用不到了.

`BPlusTree`循环调用`root`的`BulkLoad`. 如果需要分裂就分裂然后产生新的root.

```java
    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement
        assert (fillFactor > 0 && fillFactor <= 1);
        int maxRecordIndex = (int)Math.ceil(2 * metadata.getOrder() * fillFactor);
        while (this.keys.size() < maxRecordIndex && data.hasNext()) {
            Pair<DataBox, RecordId> nextData = data.next();
            keys.add(nextData.getFirst());
            rids.add(nextData.getSecond());
        }
        Optional<Pair<DataBox, Long>> res = Optional.empty();
        if (data.hasNext()) {
            // overflow
            Pair<DataBox, RecordId> nextData = data.next();
            List<DataBox> rightKeys = new ArrayList<>();
            List<RecordId> rightRids = new ArrayList<>();
            rightKeys.add(nextData.getFirst());
            rightRids.add(nextData.getSecond());
            LeafNode rightNode = new LeafNode(metadata, bufferManager, rightKeys, rightRids, rightSibling, treeContext);
            rightSibling = Optional.ofNullable(rightNode.getPage().getPageNum());
            res =  Optional.of(new Pair(nextData.getFirst(), rightSibling.get()));
        }
        sync();
        return res;
    }
```

```java
    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement
        assert (fillFactor > 0 && fillFactor <= 1);
        Optional<Pair<DataBox, Long>> res = Optional.empty();
        while (keys.size() <= 2 * metadata.getOrder() && data.hasNext()) {
            Optional<Pair<DataBox, Long>> bulkPair = getChild(children.size() - 1).bulkLoad(data, fillFactor);
            if (bulkPair.isPresent()) {
                keys.add(bulkPair.get().getFirst());
                children.add(bulkPair.get().getSecond());

                // 节点溢出
                if (keys.size() > 2 * metadata.getOrder()) {
                    res = split();
                    break;
                }
            }
        }
        sync();
        return res;
    }
```

```java
    public void bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        // TODO(proj2): implement
        // Note: You should NOT update the root variable directly.
        // Use the provided updateRoot() helper method to change
        // the tree's root if the old root splits.
        while (data.hasNext()) {
            Optional<Pair<DataBox, Long>> bulkPair = root.bulkLoad(data, fillFactor);
            if (bulkPair.isPresent()) {
                spiltRoot(bulkPair.get().getFirst(), bulkPair.get().getSecond());
            }
        }
    }
```

---

感觉坐下来之后大致懂了整个`index`部分的逻辑了...
