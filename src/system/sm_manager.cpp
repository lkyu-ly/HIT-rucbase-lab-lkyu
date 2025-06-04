/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta* new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description:
 * 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    // 数据库不存在
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    // 数据库已经打开
    if (!db_.name_.empty()) {
        throw DatabaseExistsError(db_name);
    }

    // 进入数据库目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    // 加载数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (!ifs) {
        throw UnixError();
    }
    ifs >> db_;  // 使用重载的>>操作符从文件读取数据库元数据

    // 打开所有表文件
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        fhs_[tab.name] = rm_manager_->open_file(tab.name);

        // 打开该表的所有索引
        for (auto& index : tab.indexes) {
            // 使用正确的方式获取索引名称并打开索引
            std::string index_name =
                ix_manager_->get_index_name(tab.name, index.cols);
            ihs_[index_name] = ix_manager_->open_index(tab.name, index.cols);
        }
    }

    // 打开日志文件
    disk_manager_->open_file(LOG_FILE_NAME);
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // 检查数据库是否已经打开
    if (db_.name_.empty()) {
        throw DatabaseNotFoundError(db_.name_);
    }
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();

    // 记录文件落盘
    for (auto& [_, file_handle] : fhs_) {
        rm_manager_->close_file(file_handle.get());
    }

    // 索引文件落盘
    for (auto& [_, index_handle] : ihs_) {
        ix_manager_->close_index(index_handle.get());
    }

    fhs_.clear();
    ihs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description:
 * 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type),
                                               col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string& tab_name,
                             const std::vector<ColDef>& col_defs,
                             Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size =
        curr_offset;  // record_size就是col
                      // meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 获取表的元数据
    TabMeta& tab = db_.get_table(tab_name);

    // 删除表的所有索引
    for (auto& index : tab.indexes) {
        // 获取索引名
        std::string index_name =
            ix_manager_->get_index_name(tab_name, index.cols);

        // 关闭并移除索引句柄
        if (ihs_.count(index_name) > 0) {
            ix_manager_->close_index(ihs_[index_name].get());
            ihs_.erase(index_name);
        }

        // 删除索引文件
        ix_manager_->destroy_index(tab_name, index.cols);
    }

    // 关闭表文件
    if (fhs_.count(tab_name) > 0) {
        rm_manager_->close_file(fhs_[tab_name].get());
        fhs_.erase(tab_name);
    }

    // 删除表文件
    rm_manager_->destroy_file(tab_name);

    // 从数据库元数据中删除表
    db_.tabs_.erase(tab_name);

    // 更新数据库元数据
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name,
                             const std::vector<std::string>& col_names,
                             Context* context) {
    // 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 获取表的元数据
    TabMeta& tab = db_.get_table(tab_name);

    // 检查索引是否已经存在
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }

    // 收集索引列的元数据
    std::vector<ColMeta> idx_cols;
    for (const auto& col_name : col_names) {
        auto it = std::find_if(
            tab.cols.begin(), tab.cols.end(),
            [&](const ColMeta& col) { return col.name == col_name; });
        if (it == tab.cols.end()) {
            throw ColumnNotFoundError(
                col_name);  // 修正这里，只传递col_name参数
        }
        idx_cols.push_back(*it);
    }

    // 计算索引列总长度
    int col_tot_len = 0;
    for (auto& col : idx_cols) {
        col_tot_len += col.len;
    }

    // 创建索引元数据
    IndexMeta idx_meta;
    idx_meta.tab_name = tab_name;
    idx_meta.col_tot_len = col_tot_len;
    idx_meta.col_num = idx_cols.size();
    idx_meta.cols = idx_cols;

    // 将索引元数据添加到表元数据中
    tab.indexes.push_back(idx_meta);

    // 创建索引文件
    ix_manager_->create_index(tab_name, idx_cols);

    // 打开索引文件
    std::string index_name = ix_manager_->get_index_name(tab_name, idx_cols);
    auto ih = ix_manager_->open_index(tab_name, idx_cols);

    // 将索引文件句柄添加到映射中
    ihs_[index_name] = std::move(ih);

    // 获取表文件句柄
    auto file_handle = fhs_[tab_name].get();

    // 为表中的每条记录建立索引
    for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
        auto record = file_handle->get_record(scan.rid(), context);

        // 构建索引键
        char* key = new char[col_tot_len];
        int offset = 0;
        for (auto& col : idx_cols) {
            memcpy(key + offset, record->data + col.offset, col.len);
            offset += col.len;
        }

        // 插入索引
        ihs_[index_name]->insert_entry(key, scan.rid(), context->txn_);

        delete[] key;
    }

    // 更新列的索引标志
    for (auto& col_name : col_names) {
        auto it =
            std::find_if(tab.cols.begin(), tab.cols.end(),
                         [&](ColMeta& col) { return col.name == col_name; });
        it->index = true;
    }

    // 更新数据库元数据
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name,
                           const std::vector<std::string>& col_names,
                           Context* context) {
    // 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 获取表元数据
    TabMeta& tab = db_.get_table(tab_name);

    // 检查索引是否存在
    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // 获取索引元数据
    auto idx_meta_iter = tab.get_index_meta(col_names);

    // 调用重载的 drop_index 方法完成实际删除操作
    drop_index(tab_name, idx_meta_iter->cols, context);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name,
                           const std::vector<ColMeta>& cols, Context* context) {
    // 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 获取表元数据
    TabMeta& tab = db_.get_table(tab_name);

    // 获取索引名称
    std::string index_name = ix_manager_->get_index_name(tab_name, cols);

    // 关闭索引文件
    if (ihs_.count(index_name) > 0) {
        ix_manager_->close_index(ihs_[index_name].get());
        ihs_.erase(index_name);
    }

    // 删除索引文件
    ix_manager_->destroy_index(tab_name, cols);

    // 查找并删除索引元数据
    for (auto it = tab.indexes.begin(); it != tab.indexes.end(); ++it) {
        if (it->col_num == cols.size()) {
            bool match = true;
            for (size_t i = 0; i < cols.size(); i++) {
                if (it->cols[i].name != cols[i].name) {
                    match = false;
                    break;
                }
            }
            if (match) {
                tab.indexes.erase(it);
                break;
            }
        }
    }

    // 更新列的索引标志
    for (const auto& col : cols) {
        auto it = std::find_if(tab.cols.begin(), tab.cols.end(),
                               [&](ColMeta& c) { return c.name == col.name; });
        if (it != tab.cols.end()) {
            it->index = false;
        }
    }

    // 更新数据库元数据
    flush_meta();
}