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

#include "sql/stmt/update_stmt.h"
#include "storage/table/table.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h" // 如果后续报 table 相关的错，也请一并包含

UpdateStmt::~UpdateStmt() {
    if (filter_stmt_ != nullptr) {
        delete filter_stmt_;
        filter_stmt_ = nullptr;
    }
}

RC UpdateStmt::create(Db* db, const UpdateSqlNode& update_sql, Stmt*& stmt)
{
    // 1. 校验目标表是否存在
    Table* table = db->find_table(update_sql.relation_name.c_str());
    if (nullptr == table) {
        LOG_WARN("table not exist. table=%s", update_sql.relation_name.c_str());
        return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    // 由于 UpdateSqlNode 仅支持单字段更新，去除原有的数量对比校验

    std::vector<FieldMeta*> update_fields;
    std::vector<Value> update_values;
    const TableMeta& table_meta = table->table_meta();

    // 2. 直接校验结构体中的单列是否存在
    const char* field_name = update_sql.attribute_name.c_str();
    const FieldMeta* field_meta = table_meta.field(field_name);

    if (nullptr == field_meta) {
        LOG_WARN("field not exist. field=%s", field_name);
        return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    // 将单列和单值存入向量，保持与 update_stmt 内部数据结构（可能是 vector）的一致性
    update_fields.push_back(const_cast<FieldMeta*>(field_meta));
    update_values.push_back(update_sql.value);

    // 3. 解析与绑定 WHERE 条件
    FilterStmt* filter_stmt = nullptr;
    if (!update_sql.conditions.empty()) {
        // 构造 FilterStmt::create 缺少的表映射参数
        std::unordered_map<std::string, Table*> tables;
        tables[table->name()] = table;

        // 传入 6 个正确参数：db, 默认表, 表映射指针, 条件数组首地址, 条件数量, 输出参数
        RC rc = FilterStmt::create(db,
            table,
            &tables,
            update_sql.conditions.data(),
            update_sql.conditions.size(),
            filter_stmt);
        if (rc != RC::SUCCESS) {
            return rc;
        }
    }

    // 4. 实例化语义阶段的 Stmt
    UpdateStmt* update_stmt = new UpdateStmt();
    update_stmt->table_ = table;
    update_stmt->update_fields_ = std::move(update_fields);
    update_stmt->update_values_ = std::move(update_values);
    update_stmt->filter_stmt_ = filter_stmt;

    stmt = update_stmt;
    return RC::SUCCESS;
}
