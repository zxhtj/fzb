#include "common/type/char_type.h"
#include "common/type/float_type.h"
#include "common/type/integer_type.h"
#include "common/type/data_type.h"
#include "common/value.h"
#include <cstdio>
#include <cstring>

class DateType : public DataType {
public:
    DateType() : DataType(AttrType::DATES) {}

    static bool is_valid_date(int year, int month, int day) {
        if (year < 0 || month < 1 || month > 12 || day < 1 || day > 31) return false;

        int days_in_month[] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
            days_in_month[2] = 29;
        }
        return day <= days_in_month[month];
    }

    RC to_string(const Value& val, string& result) const override {
        int32_t date_val = val.get_int();
        int year = date_val / 10000;
        int month = (date_val % 10000) / 100;
        int day = date_val % 100;

        char buffer[32];
        snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
        result = buffer;
        return RC::SUCCESS;
    }

    RC set_value_from_str(Value& val, const string& str) const override {
        int year = 0, month = 0, day = 0;

        if (sscanf(str.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
            return RC::INVALID_ARGUMENT; // <-- 已修复：使用标准的参数非法宏
        }

        if (!is_valid_date(year, month, day)) {
            return RC::INVALID_ARGUMENT; // <-- 已修复：非法日期返回参数非法宏
        }

        int32_t date_val = year * 10000 + month * 100 + day;
        // 【已修复 BUG】：直接调用 value.cpp 中新增的 set_date 方法
        // 保证底层的 attr_type_ 是 DATES 而不是 INTS
        val.set_date(date_val); 
        return RC::SUCCESS;
    }

    int compare(const Value& left, const Value& right) const override {
        int32_t l = left.get_int();
        int32_t r = right.get_int();
        if (l < r) return -1;
        if (l > r) return 1;
        return 0;
    }
};

// 按照 AttrType 枚举的顺序注册
array<unique_ptr<DataType>, static_cast<int>(AttrType::MAXTYPE)> DataType::type_instances_ = {
    make_unique<DataType>(AttrType::UNDEFINED),
    make_unique<CharType>(),
    make_unique<IntegerType>(),
    make_unique<FloatType>(),
    make_unique<DataType>(AttrType::BOOLEANS),
    make_unique<DateType>(),  // <-- 注意：放在 BOOLEANS 下面，正好对应我们在 attr_type.h 里的顺序
};