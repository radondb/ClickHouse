//#pragma once
//
//#include <Common/COW.h>
//#include <Columns/IColumn.h>
//
//namespace DB
//{
//
//class ColumnJSON final : public COWHelper<IColumn, ColumnJSON>
//{
//public:
//
//    const char * getFamilyName() const override { return "JSON"; }
//
//    size_t size() const override { return 0; }
//
//    Field operator[](size_t /*n*/) const override
//    {
//        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
//    }
//
//    void get(size_t /*n*/, Field & /*res*/) const override
//    {
//        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
//    }
//
//    StringRef getDataAt(size_t n) const override
//    {
//        return StringRef();
//    }
//
//    void insert(const Field & x) override
//    {
//
//    }
//
//    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
//    {
//
//    }
//
//    void insertData(const char * pos, size_t length) override
//    {
//
//    }
//
//    void insertDefault() override
//    {
//
//    }
//
//    void popBack(size_t n) override
//    {
//
//    }
//
//    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
//    {
//        return StringRef();
//    }
//
//    const char * deserializeAndInsertFromArena(const char * pos) override
//    {
//        return nullptr;
//    }
//
//    void updateHashWithValue(size_t n, SipHash & hash) const override
//    {
//
//    }
//
//    Ptr filter(const Filter & filt, ssize_t result_size_hint) const override
//    {
//        return COW::Ptr();
//    }
//
//    Ptr index(const IColumn & indexes, size_t limit) const override
//    {
//        return COW::Ptr();
//    }
//
//    Ptr permute(const Permutation & perm, size_t limit) const override
//    {
//        return COW::Ptr();
//    }
//
//    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override
//    {
//        return 0;
//    }
//
//    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override
//    {
//
//    }
//
//    Ptr replicate(const Offsets & offsets) const override
//    {
//        return COW::Ptr();
//    }
//
//    std::vector<MutablePtr> scatter(ColumnIndex num_columns, const Selector & selector) const override
//    {
//        return std::vector<MutablePtr>();
//    }
//
//    void gather(ColumnGathererStream & gatherer_stream) override
//    {
//
//    }
//
//    void getExtremes(Field & min, Field & max) const override
//    {
//
//    }
//
//    size_t byteSize() const override
//    {
//        return 0;
//    }
//
//    size_t allocatedBytes() const override
//    {
//        return 0;
//    }
//};
//
//}
