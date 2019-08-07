#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>
#include <Columns/ColumnsNumber.h>
#include <ext/bit_cast.h>
#include <bitset>
#include <typeinfo>


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
        bool useDefaultImplementationForConstants() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments_type) const override
        {
            if (arguments_type.empty() || arguments_type.size() > 64)
                throw Exception("Function " + getName() + " arguments range from 1 to 64", ErrorCodes::BAD_ARGUMENTS);

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

                if (which.isInt() || which.isUInt() || which.isDateOrDateTime() || which.isFloat() || which.isEnum())
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
                std::cout << "\n debug: i is " << i << "\n";
                executeArgument(arg.type.get(), arg.column.get(), vec_to, i, fixed_char_pre_row);
            }
            block.getByPosition(result).column = std::move(column_to);
        }

        void executeArgument(
                const IDataType * type, const IColumn * column,
                ColumnFixedString::Chars & vec_to, size_t argument_position, size_t fixed_char_pre_row) const {

            WhichDataType which(type);

            if (which.isUInt8()) executeForInt<UInt8>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isUInt16()) executeForInt<UInt16>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isUInt32()) executeForInt<UInt32>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isUInt64()) executeForInt<UInt64>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isInt8()) executeForInt<Int8>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isInt16()) executeForInt<Int16>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isInt32()) executeForInt<Int32>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isInt64()) executeForInt<Int64>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isEnum8()) executeForInt<Int8>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isEnum16()) executeForInt<Int16>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isDate()) executeForInt<UInt16>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isDateTime()) executeForInt<UInt32>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isFloat32()) executeForInt<Float32>(column, vec_to, argument_position, fixed_char_pre_row);
            else if (which.isFloat64()) executeForInt<Float64>(column, vec_to, argument_position, fixed_char_pre_row);
            else
                throw Exception("Unexpected type " + type->getName() + " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

    private:
        mutable size_t fold_size;
        mutable size_t arguments_size;

        template <typename FromType>
        void executeForInt(
                const IColumn * column, ColumnFixedString::Chars & vec_to,
                size_t argument_position, size_t fixed_char_pre_row) const {

            size_t max_for_type = ext::bit_cast<UInt32, FromType>(std::numeric_limits<FromType>::max());
            std::cout << "debug: max_for_type: " << max_for_type << "\n";

            if (const auto column_from = static_cast<const ColumnVector<FromType> *>(column))
            {
                const typename ColumnVector<FromType>::Container & vec_from = column_from->getData();

                for (size_t row = 0; row < vec_from.size(); ++row)
                {
                    UInt32 calc_value;
                    std::cout << "\n debug: row is " << row << "\n";
                    if constexpr (!std::is_same_v<FromType, UInt64> && !std::is_same_v<FromType, Int64> && !std::is_same_v<FromType, Float64>)
                        calc_value = ext::bit_cast<UInt32, FromType>(vec_from[row]);
                    else
                    {
                        UInt64 from_value = ext::bit_cast<UInt64, FromType>(vec_from[row]);
                        calc_value = UInt32(from_value >> 32) ^ UInt32(from_value & 0xffffffff);
                    }
                    for (size_t j = 1; j <= fold_size; ++j)
                    {
                        auto is_gt_mid = UInt8(calc_value >= (max_for_type >> j));
                        calc_value -= is_gt_mid * (max_for_type >> j);

                        std::cout << "debug: max_for_type >> (j)" << (max_for_type >> j) << "\n";
                        std::cout << "debug: is_gt_mid: " << int(is_gt_mid) <<"\n";
                        size_t bit_pos = arguments_size * ( j - 1 ) + (arguments_size - argument_position - 1);
                        std::cout << "debug: argument_position is " << size_t(argument_position)
                                  << ", arguments_size is " << size_t(arguments_size)
                                  << ", j is " << size_t(j) << "\n";
                        std::cout << "debug: bit_pos: " << size_t(bit_pos) <<"\n";
                        std::cout << " *** [bit_pos / 8 + fixed_char_pre_row * row] is " << (bit_pos / 8 + fixed_char_pre_row * row) << "\n";
                        vec_to[bit_pos / 8 + fixed_char_pre_row * row] |= is_gt_mid << (bit_pos % 8);
                        std::cout << "test: is_gt_mid << (bit_pos % 8);" << std::bitset<sizeof(int)*8>(is_gt_mid << (bit_pos % 8)) << "\n";
                        std::cout << "debug: vec_to[bit_pos / 8 + fixed_char_pre_row * row]: " << int(vec_to[bit_pos / 8 + fixed_char_pre_row * row]) <<"\n";
                        std::cout << "test_debug: binary vec_to[bit_pos / 8 + fixed_char_pre_row * row]:" << std::bitset<sizeof(int)*8>(int(vec_to[bit_pos / 8 + fixed_char_pre_row * row])) << "\n";
                    }
                }
            }
            else if (const auto col_from_const = checkAndGetColumnConst<const ColumnVector<FromType>>(&*column))
            {
                UInt32 calc_value;
                if constexpr (!std::is_same_v<FromType, UInt64> && !std::is_same_v<FromType, Int64> && !std::is_same_v<FromType, Float64>)
                    calc_value = ext::bit_cast<UInt32, FromType>(col_from_const->template getValue<FromType>());
                else
                {
                    UInt64 from_value = ext::bit_cast<UInt64, FromType>(col_from_const->template getValue<FromType>());
                    calc_value = UInt32(from_value >> 32) ^ UInt32(from_value & 0xffffffff);
                }
                for (size_t row = 0; row < col_from_const->size(); ++row)
                {
                    for (size_t j = 1; j <= fold_size; ++j)
                    {
                        auto is_gt_mid = UInt8(calc_value >= (max_for_type >> j));
                        size_t bit_pos = arguments_size * ( j - 1 ) + (arguments_size - argument_position);
                        vec_to[bit_pos / 8 + fixed_char_pre_row * row] |= is_gt_mid << (bit_pos % 8);
                    }
                }
            }
        }
    };
}
