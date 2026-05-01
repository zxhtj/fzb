#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/stmt/update_stmt.h"

class UpdateLogicalOperator : public LogicalOperator {
public:
    UpdateLogicalOperator(UpdateStmt* update_stmt)
        : update_stmt_(update_stmt) {
    }

    LogicalOperatorType type() const override {
        return LogicalOperatorType::UPDATE; // 确保枚举类LogicalOperatorType中存在UPDATE
    }

    virtual ~UpdateLogicalOperator() = default;

    UpdateStmt* update_stmt() const { return update_stmt_; }

private:
    UpdateStmt* update_stmt_ = nullptr;
};