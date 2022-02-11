本仓库为CMU-15445的教学数据库的实现。
  课程主页:https://15445.courses.cs.cmu.edu/fall2020/index.html 
  
  Gradescope:https://www.gradescope.com/   Entry Code:5VX7JZ 
  
  完成19-21年所有课程实验, Gradescope测试点均满分通过,熟读该教学数据库源码实现。 
  
  实现包括并行缓冲页面管理器（Buffer Pool Manager）,B+树索引,可扩展哈希索引,索引并发控制,请求执行(Query Execution),事务并发控制,日志恢复等功能。
系统内部使用分页存储数据,页面管理器采用读写锁以及LRU页面置换算法管理页面。采用多缓冲区与页面分区的方式,构建并行页面管理器。对B+树索引进行更新时,通过乐观和悲观两种加锁处理方式进行并发控制。通过在内存中构建哈希表实现表连接(hash join)、聚合查询(group by)以及去重(distinct)查询。采用两阶段锁(2PL)控制事务并发访问。分别采用缩点算法和WOUND-WAIT策略实现死锁检测与死锁预防两种方案管理死锁。采用WAL的方式添加日志,并实现日志异步刷盘。实现checkpoint对日志压缩。
