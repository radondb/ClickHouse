#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/Hash.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>
#include <Columns/ColumnsNumber.h>
#include <ext/bit_cast.h>


namespace DB
{
    struct ImplCityHash32
    {
        static UInt32 apply(const char * s, const size_t len)
        {
            UInt64 hash = CityHash_v1_0_2::CityHash64(s, len);
            return (hash >> 32) ^ (hash & 0xffffffff);
        }
    };

    class FunctionXZCurve : public IFunction
    {
    public:
        static constexpr auto name = "xzCurve";

        String getName() const override {
            return name;
        }

        static FunctionPtr create(const Context & /*context*/)
        {
            return std::make_shared<FunctionXZCurve>();
        }

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments_type) const override
        {
            if (arguments_type.size() > 64)
                throw Exception("Function " + getName() + " arguments size must less than 64.", ErrorCodes::BAD_ARGUMENTS);

            auto create_result_type = [&](size_t max_argument_bytes)
            {
                fold_size = max_argument_bytes * 6;
                arguments_size = arguments_type.size();
                size_t n_char = (fold_size * arguments_size + 7 ) / 8;
                return std::make_shared<DataTypeFixedString>(n_char);
            };

            size_t max_argument_bytes = 0;
            for (const auto & argument_type : arguments_type)
            {
                WhichDataType which(argument_type);

                if (which.isStringOrFixedString())
                    return create_result_type(sizeof(UInt32));
                else if (which.isInt() || which.isUInt() || which.isDateOrDateTime() || which.isFloat())
                    max_argument_bytes = std::max(max_argument_bytes, std::min(sizeof(UInt32), argument_type->getSizeOfValueInMemory()));
                else
                    throw Exception("Function " + getName()
                                    + " argument type" + argument_type->getName()
                                    + "is not suitable", ErrorCodes::BAD_ARGUMENTS);
            }

            return create_result_type(max_argument_bytes);
        }

        void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
        {
            size_t number_of_elements = arguments.size();
            size_t fixed_char_pre_row = block.getByPosition(result).type->getSizeOfValueInMemory();

            auto column_to = ColumnFixedString::create(fixed_char_pre_row);
            auto & vec_to = column_to->getChars();

            vec_to.assign(input_rows_count * fixed_char_pre_row, static_cast<UInt8>(0));

            if (arguments.empty())
                throw Exception("Function " + getName() + " arguments is empty", ErrorCodes::BAD_ARGUMENTS);

            for (size_t i = 0; i < number_of_elements; ++i)
            {
                const auto & arg = block.getByPosition(arguments[i]);
                executeforargument(input_rows_count, fixed_char_pre_row, vec_to, i, arg);
            }
            block.getByPosition(result).column = std::move(column_to);
        }

        void executeforargument(size_t input_rows_count, size_t fixed_char_pre_row, ColumnFixedString::Chars & vec_to, size_t argument_position,
                                const ColumnWithTypeAndName & arg) const {
            WhichDataType whichdatatype(arg.type);

            if (whichdatatype.isUInt8())
            {
                executeForInt<UInt8>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isUInt16())
            {
                executeForInt<UInt16>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isUInt32())
            {
                executeForInt<UInt32>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isUInt64())
            {
                executeForInt<UInt64>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isInt8())
            {
                executeForInt<Int8>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isInt16())
            {
                executeForInt<Int16>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isInt32())
            {
                executeForInt<Int32>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isInt64())
            {
                executeForInt<Int64>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isFloat32())
            {
                executeForInt<Float32>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isFloat64())
            {
                executeForInt<Float64>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isDate())
            {
                executeForInt<UInt16>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }
            else if (whichdatatype.isDateTime())
            {
                executeForInt<UInt32>(input_rows_count, fixed_char_pre_row, vec_to, argument_position, arg);
            }

        }

    private:
        mutable size_t fold_size;
        mutable size_t arguments_size;
        
        template <typename FromType>
        void executeForInt(size_t input_rows_count, size_t fixed_char_pre_row, ColumnFixedString::Chars & vec_to,
                           size_t argument_position, const ColumnWithTypeAndName &arg) const {
            
            size_t max_for_type = ext::bit_cast<UInt32, FromType>(std::numeric_limits<FromType>::max());

            if (const auto column_from = static_cast<const ColumnVector<FromType> *>(&*arg.column))
            {
                const typename ColumnVector<FromType>::Container & vec_from = column_from->getData();

                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    for (size_t j = 0; j < fold_size; ++j)
                    {
                        UInt32 calc_value;
                        if constexpr (!std::is_same_v<FromType, UInt64> && !std::is_same_v<FromType, Int64> && !std::is_same_v<FromType, Float64>)
                            calc_value = ext::bit_cast<UInt32, FromType>(vec_from[row]);
                        else
                        {
                            UInt64 from_value = ext::bit_cast<UInt64, FromType>(vec_from[row]);
                            calc_value = UInt32(from_value >> 32) ^ UInt32(from_value & 0xffffffff);
                        }

                        auto is_gt_mid = UInt8(calc_value >= max_for_type >> (j + 1));
                        size_t bit_pos = argument_position * ( j - 1 ) + (arguments_size - argument_position);
                        vec_to[bit_pos / 8 + fixed_char_pre_row * row] |= is_gt_mid << (bit_pos % 8);
                    }

                }
            }
            else if (const auto col_from_const = checkAndGetColumnConst<const ColumnVector<FromType>>(&*arg.column))
            {
                UInt32 calc_value;
                if constexpr (!std::is_same_v<FromType, UInt64> && !std::is_same_v<FromType, Int64> && !std::is_same_v<FromType, Float64>)
                    calc_value = ext::bit_cast<UInt32, FromType>(col_from_const->template getValue<FromType>());
                else
                {
                    UInt64 from_value = ext::bit_cast<UInt64, FromType>(col_from_const->template getValue<FromType>());
                    calc_value = UInt32(from_value >> 32) ^ UInt32(from_value & 0xffffffff);
                }
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    for (size_t j = 0; j < fold_size; ++j)
                    {
                        auto is_gt_mid = UInt8(calc_value >= max_for_type >> (j + 1));
                        size_t bit_pos = argument_position * ( j - 1 ) + (arguments_size - argument_position);
                        vec_to[bit_pos / 8 + fixed_char_pre_row * row] |= is_gt_mid << (bit_pos % 8);
                    }
                }
            }
        }
    };
}
