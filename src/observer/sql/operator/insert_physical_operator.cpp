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
// Created by WangYunlai on 2021/6/9.
//

#include <cstdio> // 引入 sscanf 所在的头文件

#include "sql/operator/insert_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

// 判断是否为闰年
static bool is_leap_year(int year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

// 历法合法性严格校验
static bool is_valid_date(int year, int month, int day) {
    // 校验年份与月份基础范围
    if (year < 0 || year > 9999) return false;
    if (month < 1 || month > 12) return false;
    if (day < 1) return false;

    // 定义平年各月最大天数（索引0为占位符，使1-12月对应索引1-12）
    int days_in_month[] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    // 如果是闰年，2月最大天数改为29
    if (is_leap_year(year)) {
        days_in_month[2] = 29;
    }

    // 校验天数是否超出该月最大限制
    return day <= days_in_month[month];
}

InsertPhysicalOperator::InsertPhysicalOperator(Table *table, vector<Value> &&values)
    : table_(table), values_(std::move(values))
{}

RC InsertPhysicalOperator::open(Trx* trx)
{
    const TableMeta& table_meta = table_->table_meta();
    for (size_t i = 0; i < values_.size(); ++i) {
        const FieldMeta* field = table_meta.field(i);
        Value& value = values_[i];

        // 拦截：如果表定义的列是 DATE 类型，但用户传入的是字符串 (CHARS)
        if (field->type() == AttrType::DATES && value.attr_type() == AttrType::CHARS) {
            int year = 0, month = 0, day = 0;

            // 【修改点】：将 get_string() 的结果存入局部变量，延长生命周期直到当前 if 块结束
            std::string date_str_obj = value.get_string();
            const char* date_str = date_str_obj.c_str();

            // 1. 基础的字符串格式拆分与数字提取
            if (sscanf(date_str, "%d-%d-%d", &year, &month, &day) == 3) {
                // 2. [新增逻辑] 历法合法性校验拦截
                if (!is_valid_date(year, month, day)) {
                    LOG_WARN("invalid calendar date provided: %s", date_str);
                    return RC::INVALID_ARGUMENT; // 拦截非法日期
                }

                // 3. 校验通过，转换为内部整型并覆写原 Value
                int date_int = year * 10000 + month * 100 + day;
                value.set_date(date_int);
            }
            else {
                // 转换失败，说明基础格式不符合 YYYY-MM-DD
                LOG_WARN("invalid date format: %s", date_str);
                return RC::INVALID_ARGUMENT;
            }
        }
    }

    // [原有逻辑保持不变]：此时 values_ 里的日期字符串已经被替换为了内部整型
    Record record;
    RC     rc = table_->make_record(static_cast<int>(values_.size()), values_.data(), record);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to make record. rc=%s", strrc(rc));
        return rc;
    }

    rc = trx->insert_record(table_, record);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
    }
    return rc;
}

RC InsertPhysicalOperator::next() { return RC::RECORD_EOF; }

RC InsertPhysicalOperator::close() { return RC::SUCCESS; }
