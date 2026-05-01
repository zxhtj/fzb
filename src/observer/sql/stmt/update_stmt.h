/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include <vector>
#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/field/field_meta.h"

class Table;

/**
 * @brief 更新语句的语义对象
 */
class UpdateStmt : public Stmt
{
public:
    // 【唯一且致命的修改点】：显式初始化基类 Stmt，告诉它这是一个 UPDATE 语句！
    UpdateStmt() = default;

    virtual ~UpdateStmt();

public:
    static RC create(Db* db, const UpdateSqlNode& update_sql, Stmt*& stmt);

    StmtType type() const override {
        return StmtType::UPDATE;
    }

public:
    Table* table() const { return table_; }
    const std::vector<FieldMeta*>& update_fields() const { return update_fields_; }
    const std::vector<Value>& update_values() const { return update_values_; }
    FilterStmt* filter_stmt() const { return filter_stmt_; }

private:
    Table* table_ = nullptr;
    std::vector<FieldMeta*> update_fields_;   // 需要更新的目标列元数据
    std::vector<Value> update_values_;         // 对应的新值
    FilterStmt* filter_stmt_ = nullptr;        // WHERE 过滤条件语句对象
};
