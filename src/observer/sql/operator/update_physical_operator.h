#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/stmt/update_stmt.h"

class UpdatePhysicalOperator : public PhysicalOperator {
public:
	UpdatePhysicalOperator(UpdateStmt* update_stmt);
	virtual ~UpdatePhysicalOperator() = default;

	PhysicalOperatorType type() const override {
		return PhysicalOperatorType::UPDATE; // 确保枚举类PhysicalOperatorType中存在UPDATE
	}

	virtual RC open(Trx* trx) override;
	virtual RC next() override;
	virtual RC close() override;

private:
	UpdateStmt* update_stmt_ = nullptr;
	Table* table_ = nullptr;
};