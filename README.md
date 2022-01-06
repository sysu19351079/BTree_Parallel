# BTree_Parallel

## 【代码运行步骤及参数设置】

1. 如果 `./data` 目录下没有 `dataset.csv` 文件，则先对 make_data.cpp 文件进行编译。使用命令：

   ```shell
   g++ make_data.cpp -o make_data -std=c++11
   ```

2. 如果要生成 [N] 个有序键值对数据集，则使用命令：

   ```shell
   ./make_data [N]
   ```

3. 如果已有 `dataset.csv` 文件，但是希望更新数据集，执行第 2 步。

4. 得到数据集后，使用命令：`make` 编译整个工程文件。

5. 根据 `dataset.csv` 文件中数据集的大小 [N]，执行 `run` 程序：

   - 不使用多线程：

     ```shell
     ./run 0 [N]
     ```

   - 使用 [k] 个子线程：（k > 0）

     ```shell
     ./run [k] [N]
     ```

6. 执行 `run` 后，在 `./result` 目录下：

   - 生成的 `B_tree` 文件保存有 B+ 树各节点块的信息，以二进制形式存储。
   - 生成的 `print_tree.txt` 文件保存自顶向下遍历 B+ 树各节点的数据，以文本格式存储。其中，
     1. 非叶节点输出其块号、所在层数、键值对数目及所有键值 key 和子节点的块号；
     2. 叶子节点输出其块号、所在层数、键值数目、数据项数目、所有键值 key 和所有数据项 entry_id。

7. `make clean` 清除所有已生成的目标文件。

