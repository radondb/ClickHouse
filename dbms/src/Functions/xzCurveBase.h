#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>


namespace DB
{
    template  <typename Op, typename Name>
    class FunctionXZCurveBase : public IFunction
    {
    public:
        static FunctionPtr create(const Context & context)
        {
            return std::make_shared<FunctionXZCurveBase>(context);
        }
        FunctionXZCurveBase(const Context &) {}

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        using ResultType = typename Op::ResultType;
        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            for (const auto & type : arguments)
            {
                if (!type->isValueRepresentedByInteger() &&
                    type->getTypeId() != TypeIndex::Float32 &&
                    type->getTypeId() != TypeIndex::Float64)
                {
                    throw Exception("Function " + getName() + " cannot take arguments of type " + type->getName(), ErrorCodes::LOGICAL_ERROR);
                }
            }
            return std::make_shared<DataTypeNumber<ResultType>>();
        }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t number_of_elements = arguments.size();

        using return_type = ResultType;
        auto out = ColumnVector<return_type>::create();
        auto & out_data = out->getData();
        out_data.assign(input_rows_count, static_cast<ResultType>(0));
        for (size_t i = 0; i < number_of_elements; ++i)
        {
            const auto & arg = block.getByPosition(arguments[i]);
            auto column_data = arg.column.get()->getRawData();
            auto size_per_element = arg.column.get()->sizeOfValueIfFixed();
            for (size_t j = 0; j < input_rows_count; ++j)
            {
                out_data[j] >>= 1;
                out_data[j] |= zShiftElement(
                        column_data.data + j * size_per_element,
                        size_per_element,
                        number_of_elements,
                        arg.type);
            }

        }
        block.getByPosition(result).column = std::move(out);
    }

    private:

    };
}