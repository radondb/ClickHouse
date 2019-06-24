#include <DataTypes/DataTypeJSON.h>

#include <IO/ReadHelpers.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustomSimpleTextSerialization.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Functions/SimdJSONParser.h>
#include <Common/CpuId.h>

namespace DB
{

static inline void dumpJson(SimdJSONParser::Iterator & iterator)
{
    if (SimdJSONParser::isObject(iterator)) {
        std::cout << "{";

        if (SimdJSONParser::firstObjectMember(iterator))
        {
            std::cout << SimdJSONParser::getKey(iterator);
            std::cout << ":";
            dumpJson(iterator);

            while(SimdJSONParser::nextObjectMember(iterator))
            {
                std::cout << ",";
                std::cout << SimdJSONParser::getKey(iterator);
                std::cout << ":";
                dumpJson(iterator);
            }

            iterator.up();
        }

        std::cout << "}";
    } else if (SimdJSONParser::isArray(iterator)) {
        std::cout << "[";

        if (SimdJSONParser::firstArrayElement(iterator))
        {
            dumpJson(iterator);
            while (SimdJSONParser::nextArrayElement(iterator))
            {
                std::cout << ",";
                dumpJson(iterator);
            }

            iterator.up();
        }

        std::cout << "]";
    } else {
        iterator.print(std::cout); // just print the lone value
    }
}

template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    ColumnString & column_string = static_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);

#if USE_SIMDJSON
        if (!Cpu::CpuFlagsCache::have_AVX2)
            throw Exception("Not have avx2.", ErrorCodes::LOGICAL_ERROR);

        SimdJSONParser parser;

        parser.preallocate(data.size() - old_chars_size);
        std::vector<typename SimdJSONParser::Iterator> stack;

        if (parser.parse(StringRef(data.data() + old_chars_size, data.size() - old_chars_size)))
        {
            auto root = parser.getRoot();
            dumpJson(root);
            std::cout << "\n";
        }
#endif
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

class DataTypeCustomJSONSerialization : public IDataTypeCustomTextSerialization
{
public:

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        read(column, [&](ColumnString::Chars & data) { readCSVStringInto(data, istr, settings.csv); });
    }

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const override
    {
        read(column, [&](ColumnString::Chars & data) { readJSONStringInto(data, istr); });
    }

    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const override
    {
        read(column, [&](ColumnString::Chars & data) { readQuotedStringInto<true>(data, istr); });
    }

    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const override
    {
        read(column, [&](ColumnString::Chars & data) { readEscapedStringInto(data, istr); });
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeCSVString<>(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        writeJSONString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr, settings);
    }

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeXMLString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeQuotedString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeEscapedString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }
};


void registerDataTypeJSON(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("JSON", []
    {
        return std::make_pair(DataTypeFactory::instance().get("String"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("JSON"), std::make_unique<DataTypeCustomJSONSerialization>()));
    });
}

}


