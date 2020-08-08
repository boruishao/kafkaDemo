# kafka示例代码
## 本套代码根据 kafka_2.11_2.4.1 版本列举了如下功能代码
### 消费者
* 接收数据
* 序列化器
* 手动assign分区
* 手动提交位移 同步和异步
* 指定offset
* 重平衡器
* 拦截器
* 多线程实现消费者的两种方式
### 生产者
* 发送数据 同步和异步
* 序列化器
* 拦截器
* 分区器
### 集群管理
* 通过KafkaCommand使用命令的形式操作topic
* 通过KafkaAdminClient使用api对topic进行CRUD