#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
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
                    throw Exception("", ErrorCodes::BAD_ARGUMENTS);
            }

            return create_result_type(max_argument_bytes);
        }

    void executeImpl(Block & /*block*/, const ColumnNumbers & /*arguments*/, size_t /*result*/, size_t /*input_rows_count*/) override
    {

        /**/ /*
        size_t number_of_elements = arguments.size();
        auto out = ColumnFixedString::create(result_type_bytes);

        size_t rows = input_rows_count;
        auto col_to = ColumnVector<UInt64>::create(rows);
        ColumnVector<UInt64>::Container & vec_to = col_to->getData();

        if (arguments.empty())
        {
            /// Constant random number from /dev/urandom is used as a hash value of empty list of arguments.
            vec_to.assign(rows, static_cast<UInt64>(0xe28dbde7fe22e41c));
        }

        auto & out_data = out->getChars();
        out_data.assign(input_rows_count * result_type_bytes, static_cast<UInt8>(0));


        for (size_t i = 0; i < number_of_elements; ++i)
        {

            const auto & arg = block.getByPosition(arguments[i]);
            executeForArgument(arg.type.get(), arg.column.get(), vec_to, i == number_of_elements - 1);

            out_data[j] >>= 1;
            out_data[j] |= zShiftElement(
                    column_data.data + j * size_per_element,
                    size_per_element,
                    number_of_elements,
                    arg.type);


        }
        block.getByPosition(result).column = std::move(out);
             */
    }

    private:

        template <typename FromType, bool first>
        void executeIntType(const IColumn * column, ColumnVector<UInt64>::Container & vec_to)
        {
            if (const ColumnVector<FromType> * col_from = checkAndGetColumn<ColumnVector<FromType>>(column))
            {

            }
            else if (auto col_from_const = checkAndGetColumnConst<ColumnVector<FromType>>(column))
            {

            }
            else
                throw Exception("Illegal column " + column->getName()
                                + " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);
        }

        template <bool first>
        void executeString(const IColumn * column, ColumnVector<UInt64>::Container & vec_to)
        {
            if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(column))
            {
                const typename ColumnString::Chars & data = col_from->getChars();
                const typename ColumnString::Offsets & offsets = col_from->getOffsets();
                size_t size = offsets.size();

                ColumnString::Offsets current_offset = 0;
                for (size_t i; i < size; ++i)
                {
                    const UInt32 h = ImplCityHash32::apply(
                            reinterpret_cast<const char *>(&data[current_offset]),
                            offsets[i] - current_offset - 1);

                    current_offset = offsets[i];
                }
            }
        }

        template <bool first>
        void executeAny(const IDataType * from_type, const IColumn * icolumn, ColumnVector<UInt64>::Container & vec_to)
        {
            WhichDataType which(from_type);

            if      (which.isString()) executeString<first>(icolumn, vec_to);
            else if (which.isFixedString()) executeString<first>(icolumn, vec_to);
            else if (which.isUInt64()) executeIntType<UInt64, first>(icolumn, vec_to);
            else if (which.isInt64()) executeIntType<Int64, first>(icolumn, vec_to);
        }

        void executeForArgument(const IDataType * type, const IColumn * column, ColumnVector<UInt64>::Container  & vec_to, bool & is_first)
        {
            /// Flattening of tuples.
            if (const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(column))
            {
                const auto & tuple_columns = tuple->getColumns();
                const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
                size_t tuple_size = tuple_columns.size();
                for (size_t i = 0; i < tuple_size; ++i)
                    executeForArgument(tuple_types[i].get(), tuple_columns[i].get(), vec_to, is_first);
            }
            else if (const ColumnTuple * tuple_const = checkAndGetColumnConstData<ColumnTuple>(column))
            {
                const auto & tuple_columns = tuple_const->getColumns();
                const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
                size_t tuple_size = tuple_columns.size();
                for (size_t i = 0; i < tuple_size; ++i)
                {
                    auto tmp = ColumnConst::create(tuple_columns[i], column->size());
                    executeForArgument(tuple_types[i].get(), tmp.get(), vec_to,is_first);
                }
            }
            else
            {
                if (is_first)
                    executeAny<true>(type, column, vec_to);
                else
                    executeAny<false>(type, column, vec_to);
            }

            is_first = false;
        }

        void xzSequenceCode(Block & block, const ColumnNumbers & arguments, ColumnFixedString::Chars & res, size_t & row)
        {

            for (size_t i = 0; i < arguments.size(); ++i)
            {

            }

        }

    };
}