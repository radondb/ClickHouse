#include <DataTypes/DataTypeJSON.h>

#include <IO/ReadHelpers.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustomSimpleTextSerialization.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

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

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeEscapedString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeQuotedString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        writeJSONString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr, settings);
    }

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const override
    {
        writeXMLString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    }
};


void registerDataTypeJSON(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("JSON", []
    {
        return std::make_pair(DataTypeFactory::instance().get("String"),
                              std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("JSON"),
                                                                   std::make_unique<DataTypeCustomJSONSerialization>()));
    });
}

}


