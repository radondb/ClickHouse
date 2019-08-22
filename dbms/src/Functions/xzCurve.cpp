#include <Functions/xzCurve.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
    void registerFunctionXZCurve(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionXZCurve>();
    }
}
