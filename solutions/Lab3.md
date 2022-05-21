Lab3 Joins and Query optimization
实现join算法、实现a linited version of the Selinger optimizer
对于part1需要阅读common/iterator、Join Operators、query/disk
对于part2需要阅读Scan and Special Operators, query/QueryPlan.java

Join Algorithm
实现block nested loop join, sort merge, grace hash join. 数据需要流式传输而不能把太多对象放在内存里.
代码里page代表在内存和磁盘之间传输的对象, block代表a set of pages
Task 1 Nested Loop Join
- Simple Nested Loop Join / SNLJ: 已经实现了, 可以参考
- Page Nested Loop Join / PNLJ: BNLJ的特例(B=3). 直接调用了BNLJ
- Block Nested Loop Join / BNLJ: 需要实现其中的fetchRecord方法, 得到join输出的下一个record. 两个helper methods:
    - fetchNextLeftBLock: fetch the next non-empty block of left table pages from leftIterator
    - fetchNextRightPage: fetch the next non-empty page of the right table (from rightIterator).
      首先看Simple Nested Loop Join的实现. 其实其结构很简单, 内部实现一个SNLJIterator, 内部维护两个Iterator<Record> leftSourceIterator和BacktrackingIterator<Record> rightSourceIterator, 遍历并找到符合条件的record
      需要重复遍历rightSource, 所以需要将其物化, 在构造函数中调用materialize(rightSource, transaction), 这样类中的rightSource就是一个materialOperator了, 之后调用其backtrackingIterator并mark第一个元素, 之后遍历完一轮就reset

Block nested loop join实现时就是四种情况的判断:


其的核心类是BNLJITerator
核心类的两个帮助方法的作用就是得到遍历左表block的leftBlockIterator和遍历右表的rightPageIterator. 需要调用QueryOperator的静态方法getBlockIterator, 这个方法可以指定maxPages数量然后读取这些page数的record进来. 返回一个对应的backreackingIterator
leftBlockIterator = getBlockIterator(leftSourceIterator,
getLeftSource().getSchema(), numBuffers - 2); // B-2

rightPageIterator = getBlockIterator(rightSourceIterator,
getRightSource().getSchema(), 1);

看了一下getBlockIterator的具体实现就是读取指定数量的records到内存中(具体是一个ArrayList中), 然后构建backtrackingIterator并返回.
核心方法的实现就是之前的四层循环:

具体代码层面的判断是分为4部分:
- The right page iterator has a value to yield
- The right page iterator doesn't have a value to yield but the left block iterator does
- Neither the right page nor left block iterators have values to yield, but there's more right pages
- Neither right page nor left block iterators have values nor are there more right pages, but there are still left blocks
  private Record fetchNextRecord() {
  // TODO(proj3_part1): implement
  if (leftRecord == null) {
  // the left source was empty
  return null;
  }

  while (true) {
  if (this.rightPageIterator.hasNext()) {
  Record rightRecord = rightPageIterator.next();
  if (compare(leftRecord, rightRecord) == 0) {
  return leftRecord.concat(rightRecord);
  }
  } else if (leftBlockIterator.hasNext()) {
  this.leftRecord = leftBlockIterator.next();
  this.rightPageIterator.reset(); // rightPageIterator返回原处
  } else if (rightSourceIterator.hasNext()) {
  fetchNextRightPage();
  leftBlockIterator.reset();
  leftRecord = leftBlockIterator.next();
  } else if (leftSourceIterator.hasNext()) {
  fetchNextLeftBlock();
  rightSourceIterator.reset();
  fetchNextRightPage();
  } else {
  return null;
  }
  }
  }

Task 2 Hash join
使用了common下的HashFunc.java, 是什么Postgres's hashing functions
Simple Hash Join, 在SHJOperator.java下, 已经实现了
Run类实现了Iterable<Record>, 代表disk上的一块区域, 可以从中读取record, 可以向其中追加record.
Partition类同样代表disk上的一块区域, 实现了Iterable<record>,
成员里有个Run类型的joinedRecords. 用于存答案

方法:
- Iterator(): 算子的对外暴露的核心方法, 直接返回本类的backtrackingIterator()方法的返回值.
- backtrackingIterator, 它所做的事情是返回joinRecords.iterator(). 如果joinRecords为null就调用本类的run方法
- run: 执行simple hash join算法的过程. 首先创建一个B-1大小的Partition数组, 并将其作为参数调用partition()方法. 之后遍历每一个partition, 进行buildAndProbe操作
- partition(Partition[] partitions, Iterable<Record> leftRecords): 对于左边表的所有records, 对指定的列值进行哈希并存入指定的partition中. 具体来说遍历leftRecords中的每一个record, 计算其需要进行哈希的列值的哈希值, 之后根据得到的哈希值把它加入对应的partition中
- buildAndProbe():
    - 这里build指的是在内存中建立哈希表. 读取partition中的record然后加入哈希表. 这里是一个HashMap<DataBox, List<Record>
    - Probe(探测), 对所有右边的Records, 检查内存中的HashMap里有没有对应值. 有则**将leftRecord.concat(rightRecord)**加入joinRecords.

总结一下哈希的过程: partition -> build -> probe
这里的partition相当于按照哈希来把数据"分表"(partition), 每次读一个表的数据进来在内存里再建立哈希表(build)
话说能不能再实现个布隆过滤器啥的...

Grace Hash join: 在GHJOperator下, 需要实现函数partition, buildAndProbe和run. 此外，你必须在getBreakSHJInputs和getBreakGHJInputs中提供一些输入，这些输入将分别用于测试简单哈希连接失败但Grace哈希连接通过（在testBreakSHJButPassGHJ中测试）和GHJ中断（在testGHJBreak中测试）。帮助类有query/partition.java
- partitions: 根据参数left来判断columnIndex是LeftColumnIndex的还是RightColumnIndex
- buildAndProbe: 首先判断哪一个partition的pages数量 <= B-2, 之后用其中的数据构建hashmap,遍历另一个的record做判断.
- run. 创建两个代表左右的partition, 之后遍历每一对partitions, 若两者的page数量都超过了B-2, 这时不能把partition读入内存中, 需要递归调用this.run(leftPartitions[i], rightPartitions[i], pass + 1), 把两个partitions作为参数；否则就调用buildAndProbe

Todo: 看partitions的实现, 是怎么持久化的 ->
- 首先看partition的实现, 其在初始化时调用了transaction.createTempTable, 之后在add方法中直接调用transactionContext.addRecord()方法. 最后的Iterator()方法也是直接调用了transactionContext.getRecordIterator方法
- 在Database类中的TransactionContextImpl中持久化
  Task 3 External Sort
  这部分的文档明明就写了Run做的事情就是创建一个临时的表并把数据写入, 物化的过程是由Buffer manager来实现的.
  实现方法
- sortRun 对传入的数据在内存中排序, 返回一个Run
- mergeSoeredRuns(runs) 给定一系列排好序的runs, 返回他们merge之后得到的单个run. 这里做的事情就是给定一堆Iterator, 对其中的元素作归并排序(准确说是归并排序的merge操作)
  这里实现的时候不能每一轮遍历所有的iterator然后找最小值, 因为每次调用next已经让元素后移了。 提前mark然后reset也是不行的， 因为minRecord是动态更新的。 这种对iterator作merge的情景需要一个队列来存放当前候选的record， 找到最小值后再从它对应的iterator中取出对应的下一个record进入队列。
  2333你怎么连这种东西都忘了。


public Run mergeSortedRuns(List<Run> runs) {
assert (runs.size() <= this.numBuffers - 1);
// TODO(proj3_part1): implement
Run ans = new Run(transaction, getSchema());

    // 每个runs的Iterator
    List<BacktrackingIterator<Record>> iterators = new ArrayList<>();
    for (Run run : runs) {
        iterators.add(run.iterator());
    }

    // merge过程
    Queue<Pair<Record, Integer>> queue = new PriorityQueue<>(new RecordPairComparator());
    for (int i = 0; i < iterators.size(); i++) {
        if (iterators.get(i).hasNext()) {
            queue.add(new Pair<>(iterators.get(i).next(), i));
        }
    }

    while (!queue.isEmpty()) {
        Pair<Record, Integer> topPair = queue.poll();
        int index = topPair.getSecond();
        ans.add(topPair.getFirst());
        if (iterators.get(index).hasNext()) {
            queue.add(new Pair<>(iterators.get(index).next(), index));
        }
    }

    return ans;
}

- mergePass(runs) 相当于一轮mergePass, 给定的输入是上一轮得到的runs列表, 将其按照B-1分组并对每一组作merge操作.
- sort  run external mergesort from start to finish, 返回最后排好序的数据. 首先把数据分为N / B个有序的runs, 之后只要剩余runs不止一个就递归调用mergePass
  Task 4 Sort Merge Join
  就是实现课上的这段伪代码
  sort R, S on join keys
  // 双指针合并
  令i = R的第一个tuple, j = S的第一个tuple
  while (i and j) {
  if (i > j) j++;
  if (i < j) i++;
  else if (i and j match) {
  emit(i, j);
  mark = j;
  while (i and j match) {
  emit(i, j);
  j++;
  }
  j = mark, i++;
  // 相当于是i和j分别都++
  }
  }

记录一下debug过程吧.
一开始的实现, test中join结果为10000个record我只有1000个, 原因是忘了复位rightRecord, 忘了增加LeftRecord. 修改后join结果变成了9901.说明应该是只有一些特例没有考虑到了.
后来意识到之前特判rightRecord为null时就进入reset过程, 但我在reset中只考虑了一般reset时的过程, 需要重新执行循环, 但是rightRecord为null导致的reset不需要重新执行循环, 因为已经得到答案了
草, 什么寄吧编程问题, 什么寄吧边界条件判断

所以重构了一下

private Record fetchNextRecord() {
Record ans = null;
// TODO(proj3_part1): implement

    // 首先判断rightIterat是否被mark
    if (marked) {
        if (rightRecord != null && compare(leftRecord, rightRecord) == 0) {
            ans = leftRecord.concat(rightRecord);
            // log(ans);
            updateRightRecord(); // rightRecord继续++
        } else {
            marked = false;
            rightIterator.reset();
            rightRecord = rightIterator.next();
            updateLeftRecord();
        }
    }

    while (!marked && leftRecord != null && rightRecord != null) {
        // 正常情况
        if (compare(leftRecord, rightRecord) > 0) updateRightRecord();
        else if (compare(leftRecord, rightRecord) < 0) updateLeftRecord();
        else {
            // left == right
            ans = leftRecord.concat(rightRecord);
            // log(ans);
            rightIterator.markPrev();
            marked = true;
            // 这里首先让rightRecord++
            updateRightRecord();
            break;
        }
    }
    return ans;
}

一种奇特的收获就是知道了如何改写成Java风格的Iterator
Query Optimization
动态规划...要实现:
- QueryPlan.execute: 基于System R cost-based queryoptimizer生成一个优化过的查询计划
- QueryPlan.minCostSingleAccess : 动态规划第一轮
- QueryPlan.minCostJoins 动态规划 > 1轮
  Task 5: Single Table Access Selection (Pass 1)
  提供了getEligibleIndexColumns和addEligibleSeelctions两个工具函数
  getEligibleIndexColumns, 做的事情是遍历所有的selectPredicates,
  addEligibleSelections这个工具函数, 大概作用是对应给定的Operator, 把可以挂载上的selectOperator都挂载上. 这里还提供了一个expect的参数, 可以让你跳过一个符合条件的selectPredicate
  然后这里挂载的时候, 如果是indexScan的话, 得到的所有的Record已经是符合在Index上的对应SelectPredicate了, 所以再挂载一个是多此一举, 相当于挂了一个进出比1:1的Operator
  public QueryOperator minCostSingleAccess(String table) {
  QueryOperator minOp = new SequentialScanOperator(this.transaction, table);
  int minIOCost = minOp.estimateIOCost();
  int index = -1;

  // TODO(proj3_part2): implement
  for (Integer i : getEligibleIndexColumns(table)) {
  SelectPredicate selectPredicate = selectPredicates.get(i);
  boolean hasIndex = this.transaction.indexExists(table, selectPredicate.column);
  if (hasIndex && selectPredicate.operator != PredicateOperator.NOT_EQUALS) {
  QueryOperator indexScanOperator = new IndexScanOperator(
  this.transaction,
  table,
  selectPredicate.column,
  selectPredicate.operator,
  selectPredicate.value
  );
  int indexIoCost = indexScanOperator.estimateIOCost();
  // 更新minCost和minOp
  if (minIOCost > indexIoCost) {
  minIOCost = indexIoCost;
  minOp = indexScanOperator;
  index = i;
  }
  }
  }
  minOp = addEligibleSelections(minOp, index);
  return minOp;
  }

Task 6  Join Selection (Pass i > 1)
要实现minCostJoins. 逻辑是给定上一轮的i-1的join起来的表, 对于每个Join条件遍历所有的JoinPredicats, 如果可以Join上, 就寻找Cost最小的JoinType, 并更新当前set的最佳joinOpertor
public Map<Set<String>, QueryOperator> minCostJoins(
Map<Set<String>, QueryOperator> prevMap,
Map<Set<String>, QueryOperator> pass1Map) {
Map<Set<String>, QueryOperator> result = new HashMap<>();
// TODO(proj3_part2): implement
// We provide a basic description of the logic you have to implement:
// For each set of tables in prevMap
//   For each join predicate listed in this.joinPredicates
//      Get the left side and the right side of the predicate (table name and column)
//
//      Case 1: The set contains left table but not right, use pass1Map
//              to fetch an operator to access the rightTable
//      Case 2: The set contains right table but not left, use pass1Map
//              to fetch an operator to access the leftTable.
//      Case 3: Otherwise, skip this join predicate and continue the loop.
//
//      Using the operator from Case 1 or 2, use minCostJoinType to
//      calculate the cheapest join with the new table (the one you
//      fetched an operator for from pass1Map) and the previously joined
//      tables. Then, update the result map if needed.

    // 就根据上面的注释来呗
    for (Set<String> set : prevMap.keySet()) {
        for (JoinPredicate predicate : this.joinPredicates) {
            // Get the left side and the right side of the predicates (table name and column)
            String leftTable = predicate.leftTable, rightTable = predicate.rightTable;
            String leftCloumn = predicate.leftColumn, rightColumn = predicate.rightColumn;
            Set<String> keySet = new HashSet<>(set);
            QueryOperator joinOperator = null;
            if(set.contains(leftTable) && !set.contains(rightTable)) {
                keySet.add(rightTable);
                joinOperator = minCostJoinType(prevMap.get(set),
                        pass1Map.get(Collections.singleton(rightTable)),
                        leftCloumn, rightColumn);
            } else if(set.contains(rightTable) && !set.contains(leftTable)) {
                keySet.add(leftTable);
                joinOperator = minCostJoinType(prevMap.get(set),
                        pass1Map.get(Collections.singleton(leftTable)),
                        rightColumn, leftCloumn);
            } else {
                continue;
            }

            if (!result.containsKey(keySet)) {
                result.put(keySet, joinOperator);
            } else {
                if (result.get(keySet).estimateIOCost() > joinOperator.estimateIOCost()) {
                    result.put(keySet, joinOperator);
                }
            }
        }
    }
    return result;
}

Task 7 Optimal Plan Selection
有一个helper methodminCostOperator , 传入一个Map<Set<String>, QueryOperator>, 找到map中cost最小的Operator. 但是最后没用上...
Map<Set<String>, QueryOperator> pass1Map = new HashMap<>();
Map<Set<String>, QueryOperator> result = new HashMap<>();

// pass 1
for (String table : this.tableNames) {
pass1Map.put(Collections.singleton(table), minCostSingleAccess(table));
result.put(Collections.singleton(table), minCostSingleAccess(table));
}

// pass 2..n
while (result.keySet().size() > 1) {
result = minCostJoins(result, pass1Map);
}

this.finalOperator = result.get(new HashSet<>(this.tableNames));
this.addGroupBy();
this.addProject();
this.addSort();
this.addLimit();

return finalOperator.iterator();

