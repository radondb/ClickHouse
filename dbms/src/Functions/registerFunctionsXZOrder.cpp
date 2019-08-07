namespace DB
{

    class FunctionFactory;

    void registerFunctionXZCurve(FunctionFactory & factory);

    void registerFunctionsXZOrder(FunctionFactory & factory)
    {
        registerFunctionXZCurve(factory);
    }

}
