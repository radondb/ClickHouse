#include <Functions/xzCurve.h>


namespace DB
{

    struct NameXZCurve { static constexpr auto name = "xzCurve"; };

    void XZCurveOpImpl::encode(XZCurveOpImpl::ResultType & num, const DataTypePtr & type)
    {
        auto type_id = type->getTypeId();
        // Flip the sign bit of signed integers
        if (type_id == TypeIndex::Int8)
        {
            num ^= static_cast<UInt8>(std::numeric_limits<Int8>::min());
        }
        if (type_id == TypeIndex::Int16)
        {
            num ^= static_cast<UInt16>(std::numeric_limits<Int16>::min());
        }
        if (type_id == TypeIndex::Int32)
        {
            num ^= static_cast<UInt32>(std::numeric_limits<Int32>::min());
        }
        if (type_id == TypeIndex::Int64)
        {
            num ^= static_cast<UInt64>(std::numeric_limits<Int64>::min());
        }
        // Uses a transformation from Common/RadixSort.h,
        // it inverts the sign bit for positive floats and inverts the whole number for negative floats.
        if (type_id == TypeIndex::Float32)
        {
            num = RadixSortFloatTransform<UInt32>::forward(static_cast<UInt32>(num));
        }
        if (type_id == TypeIndex::Float64)
        {
            num = RadixSortFloatTransform<UInt64>::forward(num);
        }
        // Shift the significant bits upwards
        num <<= ((sizeof(ResultType) - type->getSizeOfValueInMemory()) << 3);
    }

    using FunctionXZCurve = FunctionXZCurveBase<XZCurveOpImpl, NameXZCurve>;

    void registerFunctionXZCurve(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionXZCurve>();
    }
}