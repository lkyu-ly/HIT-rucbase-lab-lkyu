/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name,
                   std::vector<Condition> conds, std::vector<Rid> rids,
                   Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    
    // 获取待删记录Rid → 遍历索引 → 构建索引键 → 删除索引项 → 物理删除记录 →
    // 返回空
    std::unique_ptr<RmRecord> Next() override {
        // 遍历所有待删除记录的 Rid
        for (const auto &rid : rids_) {
            // 读取待删除记录
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);

            // 删除索引项:数据库需要维护数据一致性，删除记录前必须先删除其关联的索引项。
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_
                              .at(sm_manager_->get_ix_manager()->get_index_name(
                                  tab_name_, index.cols))
                              .get();

                // 键缓冲区
                char *key = new char[index.col_tot_len];
                int offset = 0;

                // 记录中提取索引键
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, rec->data + index.cols[j].offset,
                           index.cols[j].len);
                    offset += index.cols[j].len;
                }

                // 调用索引管理器删除索引项
                ih->delete_entry(key, context_->txn_);

                delete[] key;
            }

            // 删除记录本身:核心功能是标记记录槽位为空（slot为空），并更新文件头信息
            fh_->delete_record(rid, context_);
        }
        // 删除操作无实际数据返回
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};