# RucBase

```text
 ____  _   _  ____ ____    _    ____  _____
|  _ \| | | |/ ___| __ )  / \  / ___|| ____|
| |_) | | | | |   |  _ \ / _ \ \___ \|  _|
|  _ <| |_| | |___| |_) / ___ \ ___) | |___
|_| \_ \___/ \____|____/_/   \_\____/|_____|
```

[RUCBase](https://github.com/ruc-deke/rucbase-lab)是由中国人民大学数据库教学团队开发，配套教育部“101 计划”计算机核心教材《数据库管理系统原理与实现》建设的教学用数据库管理系统，旨在支撑面向本科数据库零基础学生的数据库系统课程实验教学。RUCBase 系统框架部分参考和借鉴了 CMU15-445 课程的[BusTub](https://github.com/cmu-db/bustub) 和 Standford CS346 课程的[Redbase](https://web.stanford.edu/class/cs346/2015/redbase.html)，目前中国人民大学、哈尔滨工业大学、华中科技大学、西安电子科技大学等高校使用该教学系统进行数据库内核实验。

本代码仓库是面向哈尔滨工业大学数据库系统课程教学需要定制的 RucBase 关系数据库管理系统内核实验代码框架。由本人在 AI 的辅助下完成，所有已完成项目都在本地环境完全测试通过。

## 目前完成项目（哈工大实验）

- [x] Lab0-磁盘管理器实现
- [x] Lab1-缓冲池管理器实现
- [x] Lab2-记录管理器实现
- [x] Lab3-数据定义的实现
- [x] Lab4-数据操纵的实现

## 本人实验环境

- 操作系统：Debian GNU/Linux 13 (trixie) x86_64 WSL
- 编译器：GCC
- 编程语言：C++17
- 管理工具：cmake
- 编辑器：VScode

### 依赖环境库配置

- gcc 7.1 及以上版本（要求完全支持 C++17）
- cmake 3.16 及以上版本
- flex
- bison
- readline

欲查看有关依赖运行库和编译工具的更多信息，以及如何运行的说明，请查阅[Rucbase 使用文档](docs/Rucbase使用文档.md)

欲了解如何在非 Linux 系统 PC 上部署实验环境的指导，请查阅[Rucbase 环境配置文档](docs/Rucbase环境配置文档.md)

## 实验文档索引

> 这里给出目前公开的文档分类索引

### 开发规范文档

- [Rucbase 开发文档](docs/Rucbase开发文档.md)

### 项目说明文档

- [Rucbase 环境配置文档](docs/Rucbase环境配置文档.md)
- [Rucbase 使用文档](docs/Rucbase使用文档.md)
- [Rucbase 项目结构](docs/Rucbase项目结构.pdf)
- [框架图](docs/框架图.pdf)
- [Rucbase 学生实验操作说明示例](docs/Rucbase学生实验操作说明示例.md)

### 学生实验指导书

> 哈工大实验文档位置：

- [Lab0-磁盘管理器实现](docs/hit-db-class/lab0.pdf)
- [Lab1-缓冲池管理器实现](docs/hit-db-class/lab1.pdf)
- [Lab2-记录管理器实现](docs/hit-db-class/lab2.pdf)
- [Lab3-数据定义的实现](docs/hit-db-class/lab3.pdf)
- [Lab4-数据操纵的实现](docs/hit-db-class/lab4.pdf)

## 怎么写实验报告？

我们的实验报告使用 markdown 格式，建议你自建一个 git 仓库存放作业，每写一个实验提交一次，这样，对于每一个 commit：

```bash
git diff <commit-hash>^ <path_to_file_or_dir> > labx.diff
```

git diff 会生成一个 diff 文件，标注和保存了你为了这次实验对指定文件/目录下的文件做了什么更改。如果你需要据此撰写实验报告，或是准备实验验收代码问答，你可以用**合适的方式**使用这个 diff 文件中的信息。
