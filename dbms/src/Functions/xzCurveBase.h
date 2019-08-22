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

    template  <typename Op, typename Name>
    class FunctionXZCurveBase : public IFunction
    {
    public:
        FunctionXZCurveBase(const Context &) = default;

        static FunctionPtr create(const Context & context)
        {
            return std::make_shared<FunctionXZCurveBase>(context);
        }

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments_type) const override
        {
            if (arguments_type.size() > 64)
                throw Exception("Function " + getName() + " arguments size must less than 64.", ErrorCodes::BAD_ARGUMENTS);

            auto create_result_type = [&](size_t max_argument_bytes)
            {
                size_t n_char = (max_argument_bytes * 6 * arguments_type.size() + 7 ) / 8;
                rows_result_type = std::make_shared<DataTypeFixedString>(n_char);
                return rows_result_type;
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
                                    + " argument type" + argument_type
                                    + "is not suitable", ErrorCodes::BAD_ARGUMENTS);
            }

            return create_result_type(max_argument_bytes);
        }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t rows = input_rows_count;
        size_t number_of_elements = arguments.size();

        auto out = ColumnFixedString::create(rows);
        auto col_to = ColumnVector<UInt32>::create(rows);
        ColumnVector<UInt32>::Container & vec_to = col_to->getData();

        if (arguments.empty())
        {
            // no arguments, should throw
            throw Exception("Function " + getName() + " arguments is empty", ErrorCodes::BAD_ARGUMENTS);
        }

        auto & out_data = out->getChars();
        out_data.assign(input_rows_count * result, static_cast<UInt8>(0));

        for (size_t i = 0; i < number_of_elements; ++i)
        {
            const auto & arg = block.getByPosition(arguments[i]);
            executeForArgument(arg.type.get(), arg.column.get(), vec_to);
            for (size_t j = 0; j < rows_result_type; ++j)
            {
                // 位移运算
            }
        }
        block.getByPosition(result).column = std::move(out);
    }

    private:
        size_t rows_result_type;
        template <typename FromType>
        void executeIntType(const IColumn * column, ColumnVector<UInt32>::Container & vec_to)
        {
            if (const ColumnVector<FromType> * col_from = checkAndGetColumn<ColumnVector<FromType>>(column))
            {
                const typename ColumnVector<FromType>::Container & vec_from = col_from->getData();

                size_t size = vec_from.size();
                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (std::is_same_v<FromType, UInt64>)
                    {
                        // 函数参数
                        vec_from[i] >> 32 ^ (vec_from[i] & 0xffffff);
                    }
                    // 代表函数返回值 全量 data，每次for ，指向下一个？
                    vec_to[i] |= calc_xz(vec_from[i]);
                }
            }
            else
                throw Exception("Illegal column " + column->getName()
                                + " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);
        }

        void executeString(const IColumn * column, ColumnVector<UInt32>::Container & vec_to)
        {
            if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(column))
            {
                const typename ColumnString::Chars & data = col_from->getChars();
                const typename ColumnString::Offsets & offsets = col_from->getOffsets();
                size_t size = offsets.size();

                ColumnString::Offsets current_offset = 0;
                for (size_t i = 0; i < size; ++i)
                {
                    const UInt32 hash_string = ImplCityHash32::apply(
                            reinterpret_cast<const char *>(&data[current_offset]),
                            offsets[i] - current_offset - 1);

                    current_offset = offsets[i];

                    vec_to[i] |= hash_string;
                }
            }
            else if (const ColumnFixedString * col_from_fixed = checkAndGetColumn<ColumnFixedString>(column))
            {
                const typename ColumnFixedString::Chars & data = col_from_fixed->getChars();
                size_t n = col_from_fixed->getN();
                size_t size = data.size()/n;

                for (size_t i = 0; i < size; ++i)
                {
                    const UInt32 hash_fix_string = ImplCityHash32::apply(
                            reinterpret_cast<const char *>(&data[i * n]), n);

                    vec_to[i] |= hash_fix_string;
                }
            }
            else
                throw Exception("Illegal column " + column->getName()
                                + " of first argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);
        }

        void executeAny(const IDataType * from_type, const IColumn * icolumn, ColumnVector<UInt32>::Container & vec_to)
        {
            WhichDataType which(from_type);

            if      (which.isUInt64()) executeIntType<UInt64>(icolumn, vec_to);
            else if (which.isInt64()) executeIntType<Int64>(icolumn, vec_to);
            else if (which.isUInt8()) executeIntType<UInt8>(icolumn, vec_to);
            else if (which.isUInt16()) executeIntType<UInt16>(icolumn, vec_to);
            else if (which.isUInt32()) executeIntType<UInt32>(icolumn, vec_to);
            else if (which.isUInt64()) executeIntType<UInt64>(icolumn, vec_to);
            else if (which.isInt8()) executeIntType<Int8>(icolumn, vec_to);
            else if (which.isInt16()) executeIntType<Int16>(icolumn, vec_to);
            else if (which.isInt32()) executeIntType<Int32>(icolumn, vec_to);
            else if (which.isInt64()) executeIntType<Int64>(icolumn, vec_to);
            else if (which.isEnum8()) executeIntType<Int8>(icolumn, vec_to);
            else if (which.isEnum16()) executeIntType<Int16>(icolumn, vec_to);
            else if (which.isDate()) executeIntType<UInt16>(icolumn, vec_to);
            else if (which.isDateTime()) executeIntType<UInt32>(icolumn, vec_to);
            else if (which.isFloat32()) executeIntType<Float32>(icolumn, vec_to);
            else if (which.isFloat64()) executeIntType<Float64>(icolumn, vec_to);
            else if (which.isString()) executeString(icolumn, vec_to);
            else if (which.isFixedString()) executeString(icolumn, vec_to);
            else
                throw Exception("Unexpected type " + from_type->getName() + " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        void executeForArgument(const IDataType * type, const IColumn * column, ColumnVector<UInt64>::Container  & vec_to)
        {
            executeAny(type, column, vec_to);
        }
    };
}
