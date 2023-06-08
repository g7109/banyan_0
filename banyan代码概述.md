# banyan代码结构
## 一、banyan 的主要对外接口
### 1. get_scope()
get_scope() 是 actor_base 类中的函数。

该函数的作用是根据当前actor对象的执行上下文构建作用域（Scope），并将其作为scope_builder对象返回。作用域地址的构建是通过迭代当前执行上下文的父级上下文，将对应层级的本地actor地址拷贝到作用域的地址中实现的。

### 2. load_cq_params()
load_cq_params() 是 cq0_query_helper 类中的函数。

该函数的作用是从指定的CQ参数文件中加载参数，并将这些参数存储在start_person_ids向量中。函数通过逐行读取参数文件并将读取到的参数ID添加到向量中实现。

### 3. run()
run() 是 actor_system 类中的函数。

该函数用于处理各种异常，并显示错误类型。

### 4. init_graph_store()
init_graph_store() 是 seastar 框架中的函数。

该函数通过Seastar框架提供的并行处理和异步操作功能，实现了一个初始化图形存储的函数。它通过并行处理各个本地分片，并在每个本地分片上执行初始化操作。

### 5. global_shard_id()
global_shard_id() 是定义在命名空间 brane 中的内联函数。

该函数用于返回当前线程所在的全局分片ID。

### 6. get_start_person()
get_start_person() 是 query0_helper 类中的函数。该类继承 benchmark_query_helper 类。

该函数根据使用的数据模式，返回相应的起始人物ID。

### 7. current_query_id()
current_query_id() 是 benchmark_query_helper 类中的函数。

该函数的目的是生成每个查询的唯一ID，以便在处理查询时进行标识和跟踪。

### 8. emplace_data()
emplace_data() 是 downstream_group_handler 类中的函数。

该函数的目的是将数据按目标分片放入缓冲区，并在缓冲区满时进行处理。

### 9. flush()
flush() 是 downstream_group_handler 类中的函数。

该函数的目的是将数据缓冲区中的数据发送到目标分片进行处理，并在发送完成后重置缓冲区，为下一批数据的接收做准备。

### 10. receive_eos()
receive_eos() 是 downstream_group_handler 类中的函数。

该函数的目的是向所有分片发送结束信号，通知它们数据流的结束。

### 11. enter_loop_scope()

该函数的目的是根据循环策略(BFS, DFS, FIFO)选择子作用域，用于控制并发执行的顺序。

### 12. enter_loop_instance_scope()

该函数的目的是根据循环策略(BFS, DFS, FIFO)选择子作用域，用于控制并发执行的顺序。

与11.中的函数区别：enter_loop_scope() 函数控制整个循环的执行顺序，而enter_loop_instance_scope() 函数控制单个循环实例的执行顺序。它们的作用域层级不同，分别针对整个循环和单个循环实例进行控制。

### 13. actor_engine()

该函数是 root_actor_group 类中的函数，它返回指向 root_actor_group 的引用。

root_actor_group 类继承 actor_base 类。

通过返回该引用，actor_engine 函数允许其他部分的代码使用 root_actor_group 类中的功能，例如创建新的 actor_group、添加 actor 等。这样可以直接在代码中对root_actor_group 进行操作，而无需通过指针或其他方式进行间接访问。


## 二、banyan 的主要类
### 1. actor_factory 类
actor_base 类的友元类

### 2. actor_base 类
基础的actor类，继承自seastar::task。它包含了处理actor消息和调度的基本功能，提供了一系列操作函数和成员变量来管理actor的状态、地址、消息队列等。

主要功能：

- 处理actor的消息队列：提供了消息入队和出队的函数，用于处理actor接收到的消息。
- 管理actor的状态：包括调度状态、停止状态等，提供了相关的判断和设置函数。
- 管理任务队列：用于添加和执行任务，包括普通任务和紧急任务。
- 构建作用域：通过get_scope()函数构建作用域，用于指定actor的作用域。
- 广播消息：通过broadcast()函数向所有shard中的指定地址广播消息。

### 3. actor_factory 类
主要功能：

- 作为 actor 的工厂类，负责创建和注册不同类型的 actor 实例。
- 实现了单例模式，通过 get() 方法获取工厂的单例实例。
- 维护了一个 actor 目录，存储了不同类型 actor 实例构建器函数的映射关系。
- 提供了创建和注册 actor 的方法，用于根据类型创建相应的 actor 实例。

### 4. actor_group 类

继承自 actor_base 类，表示一个 actor 组。

- 实现了对管理的多个 actor 进行调度、停止等操作。
- 使用 radix 树索引存储管理的 actor。
- 根据传入的比较器类型进行任务排序和调度。

### 5. coordinator 类

功能：协调器，用于实现全局和局部屏障，管理协调工作线程。

### 6. dynamic_queue 类

功能：表示一个异步的单生产者单消费者队列，具有有限容量。


### 7. gpu_resource_pool

功能：表示一个GPU资源池，用于管理GPU资源的分配和释放。

### 8. actor_message_queue

功能：表示一个actor消息队列，用于处理actor消息的发送和接收。

### 9. local_channel

功能：表示一个本地通道，用于发送本地和跨分片的actor消息。

### 10. root_actor_group
继承自actor_base类

主要功能：
管理actor组中的actor数量和映射关系。
提供发送消息、退出、取消查询、取消范围等功能的方法。
使用信号量控制actor的激活数量。
处理actor的停止、取消等操作。

### 11. work_thread
表示一个工作线程，用于执行提交的工作项。

主要功能：
处理工作项的执行和完成。
使用事件fd来触发工作线程的启动。
运行在一个独立的线程中。

### 12. thread_resource_pool
表示线程资源池，管理工作线程的创建和执行。


### 13. connection_manager
表示连接管理器，负责管理网络连接和通信。

主要功能：
创建服务器端和客户端的连接。
启动服务器端和客户端的运行。
管理连接的建立和关闭。
运行发送和接收循环以处理网络数据。

### 14. network_io
表示网络输入/输出(IO)模块，负责启动和连接网络。

主要功能：
启动服务器端和客户端的网络连接。
管理连接管理器的实例。
停止网络连接。

### 15. fixed_column_batch
固定列批处理类模板，用于处理具有固定列数的数据。

### 16. ynamic_column_batch
动态列批处理类模板，用于处理具有动态列数的数据。

