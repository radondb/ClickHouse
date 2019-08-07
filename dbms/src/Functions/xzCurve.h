#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>
#include <Functions/xzCurveBase.h>
#include <Common/RadixSort.h>

namespace DB
{
    struct NameXZCurve;
    struct XZCurveOpImpl
    {
        using ResultType = UInt64;

        /// Encodes
        static void encode(ResultType& num, const DataTypePtr & type);
    };

    void registerFunctionXZCurve(FunctionFactory & factory);
}
