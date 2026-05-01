#include "sql/operator/update_physical_operator.h"
#include "storage/table/table.h"
#include "storage/record/record.h"
#include "sql/expr/tuple.h"
#include <cstring>
#include <algorithm>
#include <vector>

UpdatePhysicalOperator::UpdatePhysicalOperator(UpdateStmt* update_stmt)
    : update_stmt_(update_stmt),
    table_(update_stmt->table()) {
}

RC UpdatePhysicalOperator::open(Trx* trx) {
    if (children_.empty()) {
        return RC::INTERNAL;
    }
    // 必须将 trx 透传给子算子，否则底层操作元数据或索引时会因为无事务直接 FAILURE
    return children_[0]->open(trx);
}

// 路径：miniob/src/observer/sql/operator/update_physical_operator.cpp
RC UpdatePhysicalOperator::next() {
    RC rc = RC::SUCCESS;

    // 简化 Task 结构：不再需要额外的 vector，直接让 old_record 深拷贝拥有数据
    struct UpdateTask {
        Record old_record;
    };
    std::vector<UpdateTask> tasks;

    int record_size = table_->table_meta().record_size();

    // ==========================================
    // 阶段一：纯读阶段（收集旧记录并深拷贝避免野指针）
    // ==========================================
    while (true) {
        rc = children_[0]->next();
        if (rc == RC::RECORD_EOF) {
            break;
        }
        if (rc != RC::SUCCESS) {
            return rc;
        }

        Tuple* tuple = children_[0]->current_tuple();
        if (tuple == nullptr) return RC::INTERNAL;

        RowTuple* row_tuple = static_cast<RowTuple*>(tuple);
        const Record& current_record = row_tuple->record();

        UpdateTask task;
        // 1. 浅拷贝：复制了 RID 等元数据，但 data_ 指针仍指向随时失效的 Buffer Pool
        task.old_record = current_record;

        // 2. 强制深拷贝数据！让 Record 底层 malloc 独立拥有属于自己的安全内存
        rc = task.old_record.copy_data(current_record.data(), record_size);
        if (rc != RC::SUCCESS) return rc;

        tasks.push_back(task);
    }

    // 【修正点1】：如果 tasks 为空，说明没有符合 WHERE 条件的数据。
    // 直接返回 RECORD_EOF，让网络层结束调用。
    if (tasks.empty()) {
        return RC::RECORD_EOF;
    }

    // ==========================================
    // 阶段二：纯写阶段（安全遍历并更新落盘）
    // ==========================================
    const std::vector<FieldMeta*>& fields = update_stmt_->update_fields();
    const std::vector<Value>& values = update_stmt_->update_values();

    for (size_t i = 0; i < tasks.size(); ++i) {
        UpdateTask& task = tasks[i];

        // 基于深拷贝后的安全旧数据，构造新数据缓冲区
        std::vector<char> new_data_buffer(record_size);
        char* new_data = new_data_buffer.data();
        memcpy(new_data, task.old_record.data(), record_size);

        // 获取新值并覆盖到新数据缓冲区对应偏移位置
        for (size_t j = 0; j < fields.size(); ++j) {
            FieldMeta* field = fields[j];
            const Value& val = values[j];

            if (val.attr_type() == AttrType::INTS || val.attr_type() == AttrType::DATES) {
                int32_t v = val.get_int();
                memcpy(new_data + field->offset(), &v, field->len());
            }
            else if (val.attr_type() == AttrType::FLOATS) {
                float v = val.get_float();
                memcpy(new_data + field->offset(), &v, field->len());
            }
            else if (val.attr_type() == AttrType::CHARS) {
                std::string str_val = val.get_string();
                const char* str = str_val.c_str();
                int copy_len = std::min((int)strlen(str), field->len());
                memset(new_data + field->offset(), 0, field->len());
                memcpy(new_data + field->offset(), str, copy_len);
            }
        }

        // 调用刚才在 table.cpp 中修改好的底层接口 (先删后插)
        rc = table_->update_record(task.old_record, new_data);
        if (rc != RC::SUCCESS) {
            return rc;
        }
    }

    // 【修正点2】：所有的更新全部完成，告诉外层没有结果集可供打印！
    // 此时返回 RC::RECORD_EOF 才是符合火山模型迭代器规范的正确做法。
    return RC::RECORD_EOF;
}
RC UpdatePhysicalOperator::close() {
    if (children_.empty()) {
        return RC::INTERNAL;
    }
    return children_[0]->close();
}