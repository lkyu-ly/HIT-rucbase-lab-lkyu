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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;  // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name,
                    std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @brief
     * 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {  // 循环扫描直到满足条件或文件结束
            rid_ =
                scan_->rid();  // 存储当前满足条件的元组在表中的物理位置（页号
                               // + 槽号）
            auto record = fh_->get_record(rid_, context_);
            if (!record) {
                scan_->next();
                continue;
            }
            if (conds_.empty() || eval_conds(record.get(), conds_, cols_)) {
                break;
            }
            scan_->next();
        }
        if (scan_->is_end()) {
            rid_ = Rid{-1, -1};  // 扫描结束时设置 rid_ = Rid{-1, -1}
            return;
        }
    }

    /**
     * @brief
     * 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        if (scan_->is_end()) {
            rid_ = Rid{-1, -1};  // 扫完了
            return;
        }
        scan_->next();              // 移动到下一条记录
        while (!scan_->is_end()) {  // 从当前 scan_ 位置继续扫描
            rid_ = scan_->rid();
            auto record = fh_->get_record(rid_, context_);
            if (!record) {
                scan_->next();
                continue;
            }
            if (conds_.empty() || eval_conds(record.get(), conds_, cols_)) {
                break;
            }
            scan_->next();
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {  // 获取当前元组？
        return fh_->get_record(rid_, context_);
        // return nullptr;
    }

    Rid &rid() override { return rid_; }

    bool is_end() const override { return scan_->is_end(); }  // return true;

    std::string getType() override { return "SeqScanExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

   private:
    bool eval_cond(const RmRecord *rec, const Condition &cond,
                   const std::vector<ColMeta> &rec_cols) {
        auto left_col = get_col(cols_, cond.lhs_col);
        char *left_val = rec->data + left_col->offset;

        char *right_val = nullptr;
        ColType col_type;
        int len = left_col->len;

        if (cond.is_rhs_val) {
            right_val = cond.rhs_val.raw->data;
            col_type = cond.rhs_val.type;
        } else {
            auto right_col = get_col(cols_, cond.rhs_col);
            right_val = rec->data + right_col->offset;
            col_type = right_col->type;
        }

        int cmp_result = ix_compare(left_val, right_val, col_type, len);
        switch (cond.op) {
            case OP_EQ:
                return cmp_result == 0;
            case OP_NE:
                return cmp_result != 0;
            case OP_LT:
                return cmp_result < 0;
            case OP_GT:
                return cmp_result > 0;
            case OP_LE:
                return cmp_result <= 0;
            case OP_GE:
                return cmp_result >= 0;
            default:
                return false;
        }
    }

    bool eval_conds(const RmRecord *rec, const std::vector<Condition> &conds,
                    const std::vector<ColMeta> &rec_cols) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) {
                               return eval_cond(rec, cond, rec_cols);
                           });
    }
};