#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Storages/StorageView.h>


namespace DB
{


StorageView::StorageView(
    const String & table_name_,
    const String & database_name_,
    Context & context_,
    ASTPtr & query_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    database_name(database_name_), context(context_), columns(columns_)
{
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);
    ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*create.select);

    /// If the internal query does not specify a database, retrieve it from the context and write it to the query.
    select.setDatabaseIfNeeded(database_name);

    inner_query = select;
}


BlockInputStreams StorageView::read(
    const Names & column_names,
    const ASTPtr & query,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    processed_stage = QueryProcessingStage::FetchColumns;
    ASTPtr inner_query_clone = getInnerQuery();
    return InterpreterSelectWithUnionQuery(inner_query_clone, context, column_names).executeWithoutUnion();
}


}
