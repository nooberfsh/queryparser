-- Copyright (c) 2017 Uber Technologies, Inc.
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Util.Scope
    ( runResolverWarn, runResolverWarnState, runResolverWError, runResolverWErrorState, runResolverNoWarn
    , makeResolverInfo
    , WithColumns (..)
    , queryColumnNames, tablishColumnNames
    , resolveStatement, resolveQuery, resolveQueryWithColumns, resolveSelectAndOrders, resolveCTE, resolveInsert
    , resolveInsertValues, resolveDefaultExpr, resolveDelete, resolveTruncate
    , resolveCreateTable, resolveTableDefinition, resolveColumnOrConstraint
    , resolveColumnDefinition, resolveAlterTable, resolveDropTable
    , resolveSelectColumns, resolvedTableHasName, resolvedTableHasSchema
    , resolveSelection, resolveExpr, resolveTableName, resolveDropTableName
    , resolveSchemaName
    , resolveTableRef, resolveColumnName, resolvePartition, resolveSelectFrom
    , resolveTablish, resolveJoinCondition, resolveSelectWhere, resolveSelectTimeseries
    , resolveSelectGroup, resolveSelectHaving, resolveOrder
    , selectionNames, mkTableSchemaMember
    ) where

import Data.Maybe (mapMaybe, isJust)
import Data.Either (lefts, rights)
import Data.List (find)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Tuple (swap)
import Data.Function ((&))
import Database.Sql.Type

import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Text.Lazy as TL
import           Data.Text.Lazy (Text)

import Control.Applicative (liftA2)
import Control.Monad.Identity

import Control.Arrow (first)

import Data.Proxy (Proxy (..))

import Polysemy
import Polysemy.State
import Polysemy.Reader
import Polysemy.Writer
import Polysemy.Error

makeResolverInfo :: Dialect d => Proxy d -> ResolverInfo a
makeResolverInfo dialect = ResolverInfo
    { bindings = emptyBindings
    , onCTECollision =
        if shouldCTEsShadowTables dialect
         then \ f x -> f x
         else \ _ x -> x
    , lambdaScope = []
    , selectScope = getSelectScope dialect
    , lcolumnsAreVisibleInLateralViews = areLcolumnsVisibleInLateralViews dialect
    }

makeColumnAlias 
    :: (Members (ResolverEff a) r)
    => a -> Text -> Sem r (ColumnAlias a)
makeColumnAlias r alias = ColumnAlias r alias . ColumnAliasId <$> getNextCounter
  where
    getNextCounter = modify @Integer (subtract 1) >> get @Integer

runResolverWarn 
    :: Dialect d 
    => Sem (ResolverEff a) (s a) -> Proxy d -> CatalogInterpreter -> (Either (ResolutionError a) (s a), [Either (ResolutionError a) (ResolutionSuccess a)])
runResolverWarn resolver dialect (runCatalog, catalog)
    = resolver
    & runCatalog
    & evalState catalog
    & runError
    & runReader (makeResolverInfo dialect)
    & runWriter
    & evalState 0
    & fmap swap
    & run

runResolverWarnState
    :: Dialect d 
    => Sem (ResolverEff a) (s a) -> Proxy d -> CatalogInterpreter -> (Either (ResolutionError a) (InMemoryCatalog, s a), [Either (ResolutionError a) (ResolutionSuccess a)])
runResolverWarnState resolver dialect (runCatalog, catalog)
    = resolver
    & runCatalog
    & runState catalog
    & runError
    & runReader (makeResolverInfo dialect)
    & runWriter
    & evalState 0
    & fmap swap
    & run


runResolverWError 
    :: Dialect d 
    => Sem (ResolverEff a) (s a) -> Proxy d -> CatalogInterpreter -> Either [ResolutionError a] (s a, [ResolutionSuccess a])
runResolverWError resolver dialect interpreter =
    let (result, warningsSuccesses) = runResolverWarn resolver dialect interpreter
        warnings = lefts warningsSuccesses
        successes = rights warningsSuccesses
     in case (result, warnings) of
            (Right x, []) -> Right (x, successes)
            (Right _, ws) -> Left ws
            (Left e, ws) -> Left (e:ws)

runResolverWErrorState
    :: Dialect d 
    => Sem (ResolverEff a) (s a) -> Proxy d -> CatalogInterpreter -> Either [ResolutionError a] (InMemoryCatalog, s a, [ResolutionSuccess a])
runResolverWErrorState resolver dialect interpreter =
    let (result, warningsSuccesses) = runResolverWarnState resolver dialect interpreter
        warnings = lefts warningsSuccesses
        successes = rights warningsSuccesses
     in case (result, warnings) of
            (Right (x, y), []) -> Right (x, y, successes)
            (Right _, ws) -> Left ws
            (Left e, ws) -> Left (e:ws)

runResolverNoWarn 
    :: Dialect d 
    => Sem (ResolverEff a) (s a) -> Proxy d -> CatalogInterpreter -> Either (ResolutionError a) (s a)
runResolverNoWarn resolver dialect interpreter = fst $ runResolverWarn resolver dialect interpreter


resolveStatement 
    :: (Dialect d, Members (ResolverEff a) r)
    => Statement d RawNames a -> Sem r (Statement d ResolvedNames a)
resolveStatement (QueryStmt stmt) = QueryStmt <$> resolveQuery stmt
resolveStatement (InsertStmt stmt) = InsertStmt <$> resolveInsert stmt
resolveStatement (UpdateStmt stmt) = UpdateStmt <$> resolveUpdate stmt
resolveStatement (DeleteStmt stmt) = DeleteStmt <$> resolveDelete stmt
resolveStatement (TruncateStmt stmt) = TruncateStmt <$> resolveTruncate stmt
resolveStatement (CreateTableStmt stmt) = CreateTableStmt <$> resolveCreateTable stmt
resolveStatement (AlterTableStmt stmt) = AlterTableStmt <$> resolveAlterTable stmt
resolveStatement (DropTableStmt stmt) = DropTableStmt <$> resolveDropTable stmt
resolveStatement (CreateViewStmt stmt) = CreateViewStmt <$> resolveCreateView stmt
resolveStatement (DropViewStmt stmt) = DropViewStmt <$> resolveDropView stmt
resolveStatement (CreateSchemaStmt stmt) = CreateSchemaStmt <$> resolveCreateSchema stmt
resolveStatement (GrantStmt stmt) = pure $ GrantStmt stmt
resolveStatement (RevokeStmt stmt) = pure $ RevokeStmt stmt
resolveStatement (BeginStmt info) = pure $ BeginStmt info
resolveStatement (CommitStmt info) = pure $ CommitStmt info
resolveStatement (RollbackStmt info) = pure $ RollbackStmt info
resolveStatement (ExplainStmt info stmt) = ExplainStmt info <$> resolveStatement stmt
resolveStatement (EmptyStmt info) = pure $ EmptyStmt info

resolveQuery 
    :: (Members (ResolverEff a) r)
    => Query RawNames a -> Sem r (Query ResolvedNames a)
resolveQuery = (withColumnsValue <$>) . resolveQueryWithColumns

resolveQueryWithColumns 
    :: (Members (ResolverEff a) r)
    => Query RawNames a -> Sem r (WithColumns (Query ResolvedNames) a)
resolveQueryWithColumns (QuerySelect info select) = do
    WithColumnsAndOrders select' columns _  <- resolveSelectAndOrders select []
    pure $ WithColumns (QuerySelect info select') columns
resolveQueryWithColumns (QueryExcept info Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \case
        RColumnRef QColumnName{..} -> makeColumnAlias columnNameInfo columnNameName
        RColumnAlias (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryExcept info (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryUnion info distinct Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \case
        RColumnRef QColumnName{..} -> makeColumnAlias columnNameInfo columnNameName
        RColumnAlias (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryUnion info distinct (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryIntersect info Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \case
        RColumnRef QColumnName{..} -> makeColumnAlias columnNameInfo columnNameName
        RColumnAlias (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryIntersect info (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryWith info [] query) = overWithColumns (QueryWith info []) <$> resolveQueryWithColumns query
resolveQueryWithColumns (QueryWith info (cte:ctes) query) = do
    cte' <- resolveCTE cte

    let TableAlias _ alias _ = cteAlias cte'

    updateBindings <- fmap ($ local (mapBindings $ bindCTE cte')) $ do
        exists <- catalogHasTable $ QTableName () None alias
        case exists of
            Exists -> asks onCTECollision
            DoesNotExist -> pure id

    ~(WithColumns (QueryWith _ ctes' query') columns) <- updateBindings $ resolveQueryWithColumns $ QueryWith info ctes query
    pure $ WithColumns (QueryWith info (cte':ctes') query') columns

resolveQueryWithColumns (QueryOrder info orders query) = do
    WithColumns query' columns <- resolveQueryWithColumns query

    ResolvedOrders orders' <- resolveOrders query orders

    pure $ WithColumns (QueryOrder info orders' query') columns

resolveQueryWithColumns (QueryLimit info limit query) = overWithColumns (QueryLimit info limit) <$> resolveQueryWithColumns query
resolveQueryWithColumns (QueryOffset info offset query) = overWithColumns (QueryOffset info offset) <$> resolveQueryWithColumns query


newtype ResolvedOrders a = ResolvedOrders [Order ResolvedNames a]

resolveOrders 
    :: (Members (ResolverEff a) r)
    => Query RawNames a -> [Order RawNames a] -> Sem r (ResolvedOrders a)
resolveOrders query orders = case query of
    QuerySelect _ s -> do
        -- dispatch to dialect specific binding rules :)
        WithColumnsAndOrders _ _ os <- resolveSelectAndOrders s orders
        pure $ ResolvedOrders os
    q@(QueryExcept _ _ _ _) -> do
        ~(q'@(QueryExcept _ (ColumnAliasList cs) _ _)) <- resolveQuery q
        let exprs = map (\ c@(ColumnAlias info _ _) -> ColumnExpr info $ RColumnAlias c) cs
        bindAliasedColumns (queryColumnNames q') $ ResolvedOrders <$> mapM (resolveOrder exprs) orders
    q@(QueryUnion _ _ _ _ _) -> do
        ~(q'@(QueryUnion _ _ (ColumnAliasList cs) _ _)) <- resolveQuery q
        let exprs = map (\ c@(ColumnAlias info _ _) -> ColumnExpr info $ RColumnAlias c) cs
        bindAliasedColumns (queryColumnNames q') $ ResolvedOrders <$> mapM (resolveOrder exprs) orders
    q@(QueryIntersect _ _ _ _) -> do
        ~(q'@(QueryIntersect _ (ColumnAliasList cs) _ _)) <- resolveQuery q
        let exprs = map (\ c@(ColumnAlias info _ _) -> ColumnExpr info $ RColumnAlias c) cs
        bindAliasedColumns (queryColumnNames q') $ ResolvedOrders <$> mapM (resolveOrder exprs) orders
    QueryWith _ _ _ -> error "unexpected AST: QueryOrder enclosing QueryWith"
    QueryOrder _ _ q -> do
        -- this case (nested orders) is possible in presto, but not vertica or hive
        resolveOrders q orders
    QueryLimit _ _ q -> resolveOrders q orders
    QueryOffset _ _ q -> resolveOrders q orders


bindCTE :: CTE ResolvedNames a -> Bindings a -> Bindings a
bindCTE CTE{..} =
    let columns =
            case cteColumns of
                [] -> queryColumnNames cteQuery
                cs -> map RColumnAlias cs
        cte = (cteAlias, columns)
     in \ Bindings{..} -> Bindings{boundCTEs = cte:boundCTEs, ..}


selectionNames :: Selection ResolvedNames a -> [RColumnRef a]
selectionNames (SelectExpr _ [alias] (ColumnExpr _ ref)) =
    let refName = case ref of
            RColumnRef (QColumnName _ _ name) -> name
            RColumnAlias (ColumnAlias _ name _) -> name
        ColumnAlias _ aliasName _ = alias
     in if (refName == aliasName) then [ref] else [RColumnAlias alias]
selectionNames (SelectExpr _ aliases _) = map RColumnAlias aliases
selectionNames (SelectStar _ _ (StarColumnNames referents)) = referents


selectionExprs :: Selection ResolvedNames a -> [Expr ResolvedNames a]
selectionExprs (SelectExpr info aliases _) = map (ColumnExpr info . RColumnAlias) aliases
selectionExprs (SelectStar info _ (StarColumnNames referents)) = map (ColumnExpr info) referents


queryColumnNames :: Query ResolvedNames a -> [RColumnRef a]
queryColumnNames (QuerySelect _ Select{selectCols = SelectColumns _ cols}) = cols >>= selectionNames
queryColumnNames (QueryExcept _ (ColumnAliasList cs) _ _) = map RColumnAlias cs
queryColumnNames (QueryUnion _ _ (ColumnAliasList cs) _ _) = map RColumnAlias cs
queryColumnNames (QueryIntersect _ (ColumnAliasList cs) _ _) = map RColumnAlias cs
queryColumnNames (QueryWith _ _ query) = queryColumnNames query
queryColumnNames (QueryOrder _ _ query) = queryColumnNames query
queryColumnNames (QueryLimit _ _ query) = queryColumnNames query
queryColumnNames (QueryOffset _ _ query) = queryColumnNames query


tablishColumnNames :: Tablish ResolvedNames a -> [RColumnRef a]
tablishColumnNames (TablishTable _ tablishAliases tableRef) =
    case tablishAliases of
        TablishAliasesNone -> getColumnList tableRef
        TablishAliasesT _ -> getColumnList tableRef
        TablishAliasesTC _ cAliases -> map RColumnAlias cAliases

tablishColumnNames (TablishSubQuery _ tablishAliases query) =
    case tablishAliases of
        TablishAliasesNone -> queryColumnNames query
        TablishAliasesT _ -> queryColumnNames query
        TablishAliasesTC _ cAliases -> map RColumnAlias cAliases

tablishColumnNames (TablishParenthesizedRelation _ tablishAliases relation) =
    case tablishAliases of
        TablishAliasesNone -> tablishColumnNames relation
        TablishAliasesT _ -> tablishColumnNames relation
        TablishAliasesTC _ cAliases -> map RColumnAlias cAliases

tablishColumnNames (TablishJoin _ _ _ lhs rhs) =
    tablishColumnNames lhs ++ tablishColumnNames rhs

tablishColumnNames (TablishLateralView _ LateralView{..} lhs) =
    let cols = maybe [] tablishColumnNames lhs in
    case lateralViewAliases of
        TablishAliasesNone -> cols
        TablishAliasesT _ -> cols
        TablishAliasesTC _ cAliases -> cols ++ map RColumnAlias cAliases
    

resolveSelectAndOrders 
    :: (Members (ResolverEff a) r)
    => Select RawNames a -> [Order RawNames a] -> Sem r (WithColumnsAndOrders (Select ResolvedNames) a)
resolveSelectAndOrders Select{..} orders = do
    (selectFrom', columns) <- traverse resolveSelectFrom selectFrom >>= \case
        Nothing -> pure (Nothing, [])
        Just (WithColumns selectFrom' columns) -> pure (Just selectFrom', columns)

    selectTimeseries' <- traverse (bindColumns columns . resolveSelectTimeseries) selectTimeseries

    maybeBindTimeSlice selectTimeseries' $ do
        selectCols' <- bindColumns columns $ resolveSelectColumns selectCols

        let selectedAliases = selectionNames =<< selectColumnsList selectCols'
            selectedExprs = selectionExprs =<< selectColumnsList selectCols'

        SelectScope{..} <- (\ f -> f columns selectedAliases) <$> asks selectScope

        selectHaving' <- bindForHaving $ traverse resolveSelectHaving selectHaving
        selectWhere' <- bindForWhere $ traverse resolveSelectWhere selectWhere
        selectGroup' <- bindForGroup $ traverse (resolveSelectGroup selectedExprs) selectGroup
        selectNamedWindow' <- bindForNamedWindow $ traverse resolveSelectNamedWindow selectNamedWindow
        orders' <- bindForOrder $ mapM (resolveOrder selectedExprs) orders
        let select = Select { selectCols = selectCols'
                            , selectFrom = selectFrom'
                            , selectWhere = selectWhere'
                            , selectTimeseries = selectTimeseries'
                            , selectGroup = selectGroup'
                            , selectHaving = selectHaving'
                            , selectNamedWindow = selectNamedWindow'
                            , ..
                            }
        pure $ WithColumnsAndOrders select columns orders'
  where
    maybeBindTimeSlice Nothing = id
    maybeBindTimeSlice (Just timeseries) = bindColumns [(Nothing, [RColumnAlias $ selectTimeseriesSliceName timeseries])]


resolveCTE 
    :: (Members (ResolverEff a) r)
    => CTE RawNames a -> Sem r (CTE ResolvedNames a)
resolveCTE CTE{..} = do
    cteQuery' <- resolveQuery cteQuery
    pure $ CTE
        { cteQuery = cteQuery'
        , ..
        }

resolveInsert 
    :: (Members (ResolverEff a) r)
    => Insert RawNames a -> Sem r (Insert ResolvedNames a)
resolveInsert Insert{..} = do
    insertTable'@(RTableName fqtn _) <- resolveTableName insertTable
    let insertColumns' = fmap (fmap (\uqcn -> RColumnRef $ uqcn { columnNameTable = Identity fqtn })) insertColumns
    insertValues' <- resolveInsertValues insertValues
    pure $ Insert
        { insertTable = insertTable'
        , insertColumns = insertColumns'
        , insertValues = insertValues'
        , ..
        }

resolveInsertValues 
    :: (Members (ResolverEff a) r)
    => InsertValues RawNames a -> Sem r (InsertValues ResolvedNames a)
resolveInsertValues (InsertExprValues info exprs) = InsertExprValues info <$> mapM (mapM resolveDefaultExpr) exprs
resolveInsertValues (InsertSelectValues query) = InsertSelectValues <$> resolveQuery query
resolveInsertValues (InsertDefaultValues info) = pure $ InsertDefaultValues info
resolveInsertValues (InsertDataFromFile info path) = pure $ InsertDataFromFile info path

resolveDefaultExpr 
    :: (Members (ResolverEff a) r)
    => DefaultExpr RawNames a -> Sem r (DefaultExpr ResolvedNames a)
resolveDefaultExpr (DefaultValue info) = pure $ DefaultValue info
resolveDefaultExpr (ExprValue expr) = ExprValue <$> resolveExpr expr

resolveUpdate 
    :: (Members (ResolverEff a) r)
    => Update RawNames a -> Sem r (Update ResolvedNames a)
resolveUpdate Update{..} = do
    updateTable'@(RTableName fqtn _) <- resolveTableName updateTable

    let tgtTableRef = rTableNameToRTableRef updateTable'
        tgtColRefs = getColumnList tgtTableRef
        tgtColSet = case updateAlias of
            Just alias -> (Just $ RTableAlias alias tgtColRefs, tgtColRefs)
            Nothing -> (Just tgtTableRef, tgtColRefs)

    (updateFrom', srcColSet) <- case updateFrom of
        Just tablish -> resolveTablish tablish >>= (\ (WithColumns t cs) -> return (Just t, cs))
        Nothing -> return (Nothing, [])

    updateSetExprs' <- bindColumns srcColSet $
        mapM (\(uqcn, expr) -> (RColumnRef uqcn { columnNameTable = Identity fqtn},) <$> resolveDefaultExpr expr) updateSetExprs

    updateWhere' <- bindColumns (tgtColSet:srcColSet) $ mapM resolveExpr updateWhere

    pure $ Update
        { updateTable = updateTable'
        , updateSetExprs = updateSetExprs'
        , updateFrom = updateFrom'
        , updateWhere = updateWhere'
        , ..
        }

resolveDelete 
    :: (Members (ResolverEff a) r)
    => Delete RawNames a -> Sem r (Delete ResolvedNames a)
resolveDelete (Delete info tableName expr) = do
    tableName'@(RTableName fqtn table@SchemaMember{..}) <- resolveTableName tableName
    when (tableType /= Table) $ throw $ DeleteFromView fqtn
    let QTableName tableInfo _ _ = tableName
    bindColumns [(Just $ RTableRef fqtn table, map (\ (QColumnName () None column) -> RColumnRef $ QColumnName tableInfo (pure fqtn) column) columnsList)] $ do
        expr' <- traverse resolveExpr expr
        pure $ Delete info tableName' expr'

resolveTruncate 
    :: (Members (ResolverEff a) r)
    => Truncate RawNames a -> Sem r (Truncate ResolvedNames a)
resolveTruncate (Truncate info name) = do
    name' <- resolveTableName name
    pure $ Truncate info name'

resolveCreateTable 
    :: forall d a r. (Dialect d, Members (ResolverEff a) r)
    => CreateTable d RawNames a -> Sem r (CreateTable d ResolvedNames a)
resolveCreateTable CreateTable{..} = do
    createTableName' <- catalogResolveCreateTableName createTableName 

    (WithColumns createTableDefinition' columns, schema) <- resolveTableDefinition createTableName' createTableDefinition
    let rTable = RTableName createTableName' schema
    catalogResolveCreateTable rTable $ isJust createTableIfNotExists

    bindColumns columns $ do
        createTableExtra' <- traverse (resolveCreateTableExtra (Proxy :: Proxy d)) createTableExtra
        pure $ CreateTable
            { createTableName = createTableName'
            , createTableDefinition = createTableDefinition'
            , createTableExtra = createTableExtra'
            , ..
            }


mkTableSchemaMember :: [UQColumnName ()] -> SchemaMember
mkTableSchemaMember columnsList = SchemaMember{..}
  where
    tableType = Table
    persistence = Persistent
    viewQuery = Nothing

resolveTableDefinition 
    :: (Members (ResolverEff a) r)
    => FQTableName a -> TableDefinition d RawNames a -> Sem r (WithColumns (TableDefinition d ResolvedNames) a, SchemaMember)
resolveTableDefinition fqtn (TableColumns info cs) = do
    cs' <- mapM resolveColumnOrConstraint cs
    let columns = mapMaybe columnOrConstraintToColumn $ NonEmpty.toList cs'
        table = mkTableSchemaMember $ map (\ c -> c{columnNameInfo = (), columnNameTable = None}) columns
    pure (WithColumns (TableColumns info cs') [(Just $ RTableRef fqtn table, map RColumnRef columns)], table)
  where
    columnOrConstraintToColumn (ColumnOrConstraintConstraint _) = Nothing
    columnOrConstraintToColumn (ColumnOrConstraintColumn ColumnDefinition{columnDefinitionName = QColumnName columnInfo None name}) =
        Just $ QColumnName columnInfo (pure fqtn) name


resolveTableDefinition _ (TableLike info name) = do
    name'@(RTableName _ schema) <- resolveTableName name
    pure (WithColumns (TableLike info name') [], schema)

resolveTableDefinition fqtn (TableAs info cols query) = do
    query' <- resolveQuery query
    let columns = queryColumnNames query'
        table = mkTableSchemaMember $ map toUQCN columns
        toUQCN (RColumnRef fqcn) = fqcn{columnNameInfo = (), columnNameTable = None}
        toUQCN (RColumnAlias (ColumnAlias _ cn _)) = QColumnName{..}
          where
            columnNameInfo = ()
            columnNameName = cn
            columnNameTable = None
    pure (WithColumns (TableAs info cols query') [(Just $ RTableRef fqtn table, columns)], table)

resolveTableDefinition _ (TableNoColumnInfo info) = do
    pure (WithColumns (TableNoColumnInfo info) [], mkTableSchemaMember [])

resolveColumnOrConstraint 
    :: (Members (ResolverEff a) r)
    => ColumnOrConstraint d RawNames a -> Sem r (ColumnOrConstraint d ResolvedNames a)
resolveColumnOrConstraint (ColumnOrConstraintColumn column) = ColumnOrConstraintColumn <$> resolveColumnDefinition column
resolveColumnOrConstraint (ColumnOrConstraintConstraint constraint) = pure $ ColumnOrConstraintConstraint constraint


resolveColumnDefinition 
    :: (Members (ResolverEff a) r)
    => ColumnDefinition d RawNames a -> Sem r (ColumnDefinition d ResolvedNames a)
resolveColumnDefinition ColumnDefinition{..} = do
    columnDefinitionDefault' <- traverse resolveExpr columnDefinitionDefault
    pure $ ColumnDefinition
        { columnDefinitionDefault = columnDefinitionDefault'
        , ..
        }


resolveAlterTable 
    :: (Members (ResolverEff a) r)
    => AlterTable RawNames a -> Sem r (AlterTable ResolvedNames a)
resolveAlterTable (AlterTableRenameTable info old new) = do
    old'@(RTableName (QTableName _ (Identity oldSchema@(QSchemaName _ (Identity oldDb@(DatabaseName _ _)) _ oldSchemaType)) _) table) <- resolveTableName old

    let new'@(RTableName (QTableName _ (Identity (QSchemaName _ _ _ newSchemaType)) _) _) = case new of
            QTableName tInfo (Just (QSchemaName sInfo (Just db) s sType)) t ->
                RTableName (QTableName tInfo (pure (QSchemaName sInfo (pure db) s sType)) t) table

            QTableName tInfo (Just (QSchemaName sInfo Nothing s sType)) t ->
                RTableName (QTableName tInfo (pure (QSchemaName sInfo (pure oldDb) s sType)) t) table

            QTableName tInfo Nothing t ->
                RTableName (QTableName tInfo (pure oldSchema) t) table

    case (oldSchemaType, newSchemaType) of
        (NormalSchema, NormalSchema) -> pure ()
        (SessionSchema, SessionSchema) -> pure ()
        (NormalSchema, SessionSchema) -> error "can't rename a table into the session schema"
        (SessionSchema, NormalSchema) -> error "can't rename a table out of the session schema"

    pure $ AlterTableRenameTable info old' new'

resolveAlterTable (AlterTableRenameColumn info table old new) = do
    table' <- resolveTableName table
    pure $ AlterTableRenameColumn info table' old new
resolveAlterTable (AlterTableAddColumns info table columns) = do
    table' <- resolveTableName table
    pure $ AlterTableAddColumns info table' columns


resolveDropTable 
    :: (Members (ResolverEff a) r)
    => DropTable RawNames a -> Sem r (DropTable ResolvedNames a)
resolveDropTable DropTable{..} = do
    dropTableNames' <- mapM (`resolveDropTableName` dropTableIfExists) dropTableNames
    pure $ DropTable
        { dropTableNames = dropTableNames'
        , ..
        }


resolveCreateView 
    :: (Members (ResolverEff a) r)
    => CreateView RawNames a -> Sem r (CreateView ResolvedNames a)
resolveCreateView CreateView{..} = do
    createViewName' <- catalogResolveCreateTableName createViewName
    createViewQuery' <- resolveQuery createViewQuery
    pure $ CreateView
        { createViewName = createViewName'
        , createViewQuery = createViewQuery'
        , ..
        }


resolveDropView 
    :: (Members (ResolverEff a) r)
    => DropView RawNames a -> Sem r (DropView ResolvedNames a)
resolveDropView DropView{..} = do
    dropViewName' <- resolveDropTableName dropViewName dropViewIfExists
    pure $ DropView
        { dropViewName = dropViewName'
        , ..
        }


resolveCreateSchema 
    :: (Members (ResolverEff a) r)
    => CreateSchema RawNames a -> Sem r (CreateSchema ResolvedNames a)
resolveCreateSchema CreateSchema{..} = do
    createSchemaName' <- catalogResolveCreateSchemaName createSchemaName
    let ret = CreateSchema
                { createSchemaName = createSchemaName'
                , ..
                }
    catalogResolveCreateSchema createSchemaName' $ isJust createSchemaIfNotExists
    pure ret


resolveSelectColumns 
    :: (Members (ResolverEff a) r)
    => SelectColumns RawNames a -> Sem r (SelectColumns ResolvedNames a)
resolveSelectColumns (SelectColumns info selections) = SelectColumns info <$> mapM resolveSelection selections


qualifiedOnly :: [(Maybe a, b)] -> [(a, b)]
qualifiedOnly = mapMaybe (\(mTable, cs) -> case mTable of
                              (Just t) -> Just (t, cs)
                              Nothing -> Nothing)

resolveSelection 
    :: (Members (ResolverEff a) r)
    => Selection RawNames a -> Sem r (Selection ResolvedNames a)
resolveSelection (SelectStar info Nothing Unused) = do
    columns:|_ <- asks (boundColumns . bindings)
    pure $ SelectStar info Nothing $ StarColumnNames $ map (const info <$>) $ snd =<< columns

resolveSelection (SelectStar info (Just oqtn@(QTableName _ (Just schema) _)) Unused) = do
    columns:|_ <- asks (boundColumns . bindings)
    let qualifiedColumns = qualifiedOnly columns
    case filter ((liftA2 (&&) (resolvedTableHasSchema schema) (resolvedTableHasName oqtn)) . fst) qualifiedColumns of
        [] -> throw $ UnintroducedTable oqtn
        [(t, cs)] -> pure $ SelectStar info (Just t) $ StarColumnNames $ map (const info <$>) cs
        _ -> throw $ AmbiguousTable oqtn

resolveSelection (SelectStar info (Just oqtn@(QTableName tableInfo Nothing table)) Unused) = do
    columns:|_ <- asks (boundColumns . bindings)
    let qualifiedColumns = qualifiedOnly columns
    case filter (resolvedTableHasName oqtn . fst) qualifiedColumns of
        [] -> throw $ UnintroducedTable $ QTableName tableInfo Nothing table
        [(t, cs)] -> pure $ SelectStar info (Just t) $ StarColumnNames $ map (const info <$>) cs
        _ -> throw $ AmbiguousTable $ QTableName tableInfo Nothing table

resolveSelection (SelectExpr info alias expr) = SelectExpr info alias <$> resolveExpr expr


resolveExpr 
    :: (Members (ResolverEff a) r)
    => Expr RawNames a -> Sem r (Expr ResolvedNames a)
resolveExpr (BinOpExpr info op lhs rhs) = BinOpExpr info op <$> resolveExpr lhs <*> resolveExpr rhs

resolveExpr (CaseExpr info whens else_) = CaseExpr info <$> mapM resolveWhen whens <*> traverse resolveExpr else_
  where
    resolveWhen (when_, then_) = (,) <$> resolveExpr when_ <*> resolveExpr then_

resolveExpr (UnOpExpr info op expr) = UnOpExpr info op <$> resolveExpr expr
resolveExpr (LikeExpr info op escape pattern expr) = do
    escape' <- traverse (fmap Escape . resolveExpr . escapeExpr) escape
    pattern' <- Pattern <$> resolveExpr (patternExpr pattern)
    expr' <- resolveExpr expr
    pure $ LikeExpr info op escape' pattern' expr'

resolveExpr (ConstantExpr info constant) = pure $ ConstantExpr info constant
resolveExpr (ColumnExpr info column) = resolveLambdaParamOrColumnName info column
resolveExpr (InListExpr info list expr) = InListExpr info <$> mapM resolveExpr list <*> resolveExpr expr
resolveExpr (InSubqueryExpr info query expr) = do
    query' <- bindNewScope $ resolveQuery query
    expr' <- resolveExpr expr
    pure $ InSubqueryExpr info query' expr'

resolveExpr (BetweenExpr info expr start end) =
    BetweenExpr info <$> resolveExpr expr <*> resolveExpr start <*> resolveExpr end

resolveExpr (OverlapsExpr info range1 range2) = OverlapsExpr info <$> resolveRange range1 <*> resolveRange range2
  where
    resolveRange (from, to) = (,) <$> resolveExpr from <*> resolveExpr to

resolveExpr (FunctionExpr info name distinct args params filter' over) =
    FunctionExpr info name distinct <$> mapM resolveExpr args <*> mapM resolveParam params <*> traverse resolveFilter filter' <*> traverse resolveOverSubExpr over
  where
    resolveParam (param, expr) = (param,) <$> resolveExpr expr
    -- T482568: expand named windows on resolve
    resolveOverSubExpr (OverWindowExpr i window) =
      OverWindowExpr i <$> resolveWindowExpr window
    resolveOverSubExpr (OverWindowName i windowName) =
      pure $ OverWindowName i windowName
    resolveOverSubExpr (OverPartialWindowExpr i partWindow) =
      OverPartialWindowExpr i <$> resolvePartialWindowExpr partWindow
    resolveFilter (Filter i expr) =
      Filter i <$> resolveExpr expr

resolveExpr (AtTimeZoneExpr info expr tz) = AtTimeZoneExpr info <$> resolveExpr expr <*> resolveExpr tz
resolveExpr (SubqueryExpr info query) = SubqueryExpr info <$> bindNewScope (resolveQuery query)
resolveExpr (ArrayExpr info array) = ArrayExpr info <$> mapM resolveExpr array
resolveExpr (ExistsExpr info query) = ExistsExpr info <$> resolveQuery query
resolveExpr (FieldAccessExpr info expr field) = FieldAccessExpr info <$> resolveExpr expr <*> pure field
resolveExpr (ArrayAccessExpr info expr idx) = ArrayAccessExpr info <$> resolveExpr expr <*> resolveExpr idx
resolveExpr (TypeCastExpr info onFail expr type_) = TypeCastExpr info onFail <$> resolveExpr expr <*> pure type_
resolveExpr (VariableSubstitutionExpr info) = pure $ VariableSubstitutionExpr info
resolveExpr (LambdaParamExpr info param) = pure $ LambdaParamExpr info param
resolveExpr (LambdaExpr info params body) = do
    expr <- bindLambdaParams params $ resolveExpr body
    pure $ LambdaExpr info params expr

resolveOrder 
    :: (Members (ResolverEff a) r)
    => [Expr ResolvedNames a]
    -> Order RawNames a
    -> Sem r (Order ResolvedNames a)
resolveOrder exprs (Order i posOrExpr direction nullPos) =
    Order i <$> resolvePositionOrExpr exprs posOrExpr <*> pure direction <*> pure nullPos

resolveWindowExpr 
    :: (Members (ResolverEff a) r)
    => WindowExpr RawNames a -> Sem r (WindowExpr ResolvedNames a)
resolveWindowExpr WindowExpr{..} =
  do
    windowExprPartition' <- traverse resolvePartition windowExprPartition
    windowExprOrder' <- mapM (resolveOrder []) windowExprOrder
    pure $ WindowExpr
        { windowExprPartition = windowExprPartition'
        , windowExprOrder = windowExprOrder'
        , ..
        }

resolvePartialWindowExpr 
    :: (Members (ResolverEff a) r)
    => PartialWindowExpr RawNames a -> Sem r (PartialWindowExpr ResolvedNames a)
resolvePartialWindowExpr PartialWindowExpr{..} =
  do
    partWindowExprOrder' <- mapM (resolveOrder []) partWindowExprOrder
    partWindowExprPartition' <- mapM resolvePartition partWindowExprPartition
    pure $ PartialWindowExpr
        { partWindowExprOrder = partWindowExprOrder'
        , partWindowExprPartition = partWindowExprPartition'
        , ..
        }

resolveNamedWindowExpr 
    :: (Members (ResolverEff a) r)
    => NamedWindowExpr RawNames a -> Sem r (NamedWindowExpr ResolvedNames a)
resolveNamedWindowExpr (NamedWindowExpr info name window) =
  NamedWindowExpr info name <$> resolveWindowExpr window
resolveNamedWindowExpr (NamedPartialWindowExpr info name partWindow) =
  NamedPartialWindowExpr info name <$> resolvePartialWindowExpr partWindow

resolveTableName 
    :: (Members (ResolverEff a) r)
    => OQTableName a -> Sem r (RTableName a)
resolveTableName table = do
    catalogResolveTableName table

resolveDropTableName 
    :: (Members (ResolverEff a) r)
    => DropTableName RawNames a -> Maybe a -> Sem r (DropTableName ResolvedNames a)
resolveDropTableName tableName ifExists = do
    catalogResolveDropTable tableName (isJust ifExists)

resolveSchemaName 
    :: (Members (ResolverEff a) r)
    => SchemaName RawNames a -> Sem r (SchemaName ResolvedNames a)
resolveSchemaName schemaName = do
    catalogResolveSchemaName schemaName

resolveTableRef 
    :: (Members (ResolverEff a) r)
    => OQTableName a -> Sem r (WithColumns RTableRef a)
resolveTableRef oqtn@(QTableName _ Nothing _) = do
    ResolverInfo{bindings = Bindings{..}} <- ask
    case filter (resolvedTableHasName oqtn) $ map (uncurry RTableAlias) boundCTEs of
        [t] -> do
            tell [Right $ TableRefResolved oqtn t]
            pure $ WithColumns t [(Just t, getColumnList t)]
        _:_ -> throw $ AmbiguousTable oqtn
        [] -> resolveTableRefInCatalog oqtn

resolveTableRef oqtn@(QTableName _ (Just _) _) = resolveTableRefInCatalog oqtn

resolveTableRefInCatalog
    :: (Members (ResolverEff a) r)
    => OQTableName a -> Sem r (WithColumns RTableRef a)
resolveTableRefInCatalog oqtn@(QTableName tInfo _ _) = do
        RTableName fqtn table@SchemaMember{..} <- catalogResolveTableName oqtn
        let makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
            tableRef = RTableRef fqtn table
        pure $ WithColumns tableRef [(Just tableRef, map makeRColumnRef columnsList)]

resolveColumnName 
    :: (Members (ResolverEff a) r)
    => OQColumnName a -> Sem r (RColumnRef a)
resolveColumnName columnName = do
    Bindings{..} <- asks bindings
    catalogResolveColumnName boundColumns columnName

resolveLambdaParamOrColumnName 
    :: (Members (ResolverEff a) r)
    => a -> OQColumnName a -> Sem r (Expr ResolvedNames a)
resolveLambdaParamOrColumnName info columnName = do
    params <- asks lambdaScope
    case isParam params columnName of 
        Just name -> pure $ LambdaParamExpr info name
        Nothing -> ColumnExpr info <$> resolveColumnName columnName
  where
    isParam :: [[LambdaParam a]] -> OQColumnName a -> Maybe (LambdaParam a)
    isParam _ (QColumnName _ (Just _) _) = Nothing
    isParam params (QColumnName _ Nothing name) = find (\(LambdaParam _ pname _) -> pname == name) $ concat params


resolvePartition 
    :: (Members (ResolverEff a) r)
    => Partition RawNames a -> Sem r (Partition ResolvedNames a)
resolvePartition (PartitionBy info exprs) = PartitionBy info <$> mapM resolveExpr exprs
resolvePartition (PartitionBest info) = pure $ PartitionBest info
resolvePartition (PartitionNodes info) = pure $ PartitionNodes info


resolveSelectFrom 
    :: (Members (ResolverEff a) r)
    => SelectFrom RawNames a -> Sem r (WithColumns (SelectFrom ResolvedNames) a)
resolveSelectFrom (SelectFrom info tablishes) = do
    tablishesWithColumns <- mapM resolveTablish tablishes
    let (tablishes', css) = unzip $ map (\ (WithColumns t cs) -> (t, cs)) tablishesWithColumns
    pure $ WithColumns (SelectFrom info tablishes') $ concat css


resolveTablish 
    :: (Members (ResolverEff a) r)
    => Tablish RawNames a -> Sem r (WithColumns (Tablish ResolvedNames) a)
resolveTablish (TablishTable info aliases name) = do
    WithColumns name' columns <- resolveTableRef name

    let columns' = case aliases of
            TablishAliasesNone -> columns
            TablishAliasesT t -> map (first $ const $ Just $ RTableAlias t (getColumnList name')) columns
            TablishAliasesTC t cs -> [(Just $ RTableAlias t (getColumnList name'), map RColumnAlias cs)]

    pure $ WithColumns (TablishTable info aliases name') columns'


resolveTablish (TablishSubQuery info aliases query) = do
    query' <- bindNewScope $ resolveQuery query
    let columns = queryColumnNames query'
        (tAlias, cAliases) = case aliases of
            TablishAliasesNone -> (Nothing, columns)
            TablishAliasesT t -> (Just $ RTableAlias t columns, columns)
            TablishAliasesTC t cs -> (Just $ RTableAlias t columns, map RColumnAlias cs)

    pure $ WithColumns (TablishSubQuery info aliases query') [(tAlias, cAliases)]

resolveTablish (TablishParenthesizedRelation info aliases relation) = do
    WithColumns relation' columns <- resolveTablish relation
    let colRefs = concatMap snd columns
        columns' = case aliases of
            TablishAliasesNone -> columns
            TablishAliasesT t -> map (first $ const $ Just $ RTableAlias t colRefs) columns
            TablishAliasesTC t cs -> [(Just $ RTableAlias t colRefs, map RColumnAlias cs)]

    pure $ WithColumns (TablishParenthesizedRelation info aliases relation') columns'

resolveTablish (TablishJoin info joinType cond lhs rhs) = do
    WithColumns lhs' lcolumns <- resolveTablish lhs

    -- special case for Presto
    lcolumnsAreVisible <- asks lcolumnsAreVisibleInLateralViews
    let bindForRhs = case (lcolumnsAreVisible, rhs) of
          (True, TablishLateralView _ _ _) -> bindColumns lcolumns
          _ -> id

    WithColumns rhs' rcolumns <- bindForRhs $ resolveTablish rhs
    let colsForRestOfQuery = case joinType of
          -- for LEFT SEMI JOIN (Hive), the rhs is only in scope in the expr, nowhere else in the query
          JoinSemi _ -> lcolumns
          _ -> lcolumns ++ rcolumns
    bindColumns (lcolumns ++ rcolumns) $ do
        cond' <- resolveJoinCondition cond lcolumns rcolumns
        pure $ WithColumns (TablishJoin info joinType cond' lhs' rhs') colsForRestOfQuery

resolveTablish (TablishLateralView info LateralView{..} lhs) = do
    (lhs', lcolumns) <- case lhs of
        Nothing -> return (Nothing, [])
        Just tablish -> do
                            WithColumns lhs' lcolumns <- resolveTablish tablish
                            return (Just lhs', lcolumns)

    bindColumns lcolumns $ do
        lateralViewExprs' <- mapM resolveExpr lateralViewExprs
        let view = LateralView
                { lateralViewExprs = lateralViewExprs'
                , ..
                }

        defaultCols <- map RColumnAlias . concat <$> mapM defaultAliases lateralViewExprs'
        let rcolumns = case lateralViewAliases of
                TablishAliasesNone -> [(Nothing, defaultCols)]
                TablishAliasesT t -> [(Just $ RTableAlias t defaultCols, defaultCols)]
                TablishAliasesTC t cs -> [(Just $ RTableAlias t defaultCols, map RColumnAlias cs)]

        pure $ WithColumns (TablishLateralView info view lhs') $ lcolumns ++ rcolumns
  where
    defaultAliases
        :: forall a r. (Members (ResolverEff a) r)
        => Expr ResolvedNames a -> Sem r [ColumnAlias a]
    defaultAliases (FunctionExpr r (QFunctionName _ _ rawName) _ args _ _ _) = do
        let argsLessOne = length args - 1

            alias = makeColumnAlias r

            prependAlias 
                :: (Members (ResolverEff a) r)
                => Text -> Int -> Sem r (ColumnAlias a)
            prependAlias prefix int = alias $ prefix `TL.append` (TL.pack $ show int)

            name = TL.toLower rawName

            functionSpecificLookups
              | name == "explode" = map alias [ "col", "key", "val" ]
              | name == "inline" = map alias [ "col1", "col2" ]
              | name == "json_tuple" = map (prependAlias "c") $ take argsLessOne [0..]
              | name == "parse_url_tuple" = map (prependAlias "c") $ take argsLessOne [0..]
              | name == "posexplode" = map alias [ "pos", "val" ]
              | name == "stack" =
                  let n = case head args of
                            (ConstantExpr _ (NumericConstant _ nText)) -> read $ TL.unpack nText
                            _ -> argsLessOne -- this should never happen, but if it does, this is a reasonable guess
                      k = argsLessOne
                      len = (k `div` n) + (if k `mod` n == 0 then 0 else 1)
                   in map (prependAlias "col") $ take len [0..]
              | otherwise = []

        sequence functionSpecificLookups

    defaultAliases _ = throw MissingFunctionExprForLateralView


resolveJoinCondition 
    :: (Members (ResolverEff a) r)
    => JoinCondition RawNames a -> ColumnSet a -> ColumnSet a -> Sem r (JoinCondition ResolvedNames a)
resolveJoinCondition (JoinNatural info _) lhs rhs = do
    let name (RColumnRef (QColumnName _ _ column)) = column
        name (RColumnAlias (ColumnAlias _ alias _)) = alias
        columns = RNaturalColumns $ do
            l <- snd =<< lhs
            r <- snd =<< rhs
            if name l == name r
             then [RUsingColumn l r]
             else []
    pure $ JoinNatural info columns

resolveJoinCondition (JoinOn expr) _ _ = JoinOn <$> resolveExpr expr
resolveJoinCondition (JoinUsing info cols) lhs rhs = JoinUsing info <$> mapM resolveColumn cols
  where
    resolveColumn (QColumnName columnInfo _ column) = do
        let resolveIn columns =
                case filter hasName $ snd =<< columns of
                    [] -> throw $ MissingColumn $ QColumnName columnInfo Nothing column
                    [c] -> pure c
                    _ -> throw $ AmbiguousColumn $ QColumnName columnInfo Nothing column
            hasName (RColumnRef (QColumnName _ _ column')) = column' == column
            hasName (RColumnAlias (ColumnAlias _ column' _)) = column' == column
        l <- resolveIn lhs
        r <- resolveIn rhs
        pure $ RUsingColumn l r


resolveSelectWhere 
    :: (Members (ResolverEff a) r)
    => SelectWhere RawNames a -> Sem r (SelectWhere ResolvedNames a)
resolveSelectWhere (SelectWhere info expr) = SelectWhere info <$> resolveExpr expr

resolveSelectTimeseries 
    :: (Members (ResolverEff a) r)
    => SelectTimeseries RawNames a -> Sem r (SelectTimeseries ResolvedNames a)
resolveSelectTimeseries SelectTimeseries{..} = do
    selectTimeseriesPartition' <- traverse resolvePartition selectTimeseriesPartition
    selectTimeseriesOrder' <- resolveExpr selectTimeseriesOrder
    pure $ SelectTimeseries
        { selectTimeseriesPartition = selectTimeseriesPartition'
        , selectTimeseriesOrder = selectTimeseriesOrder'
        , ..
        }

resolvePositionOrExpr 
    :: (Members (ResolverEff a) r)
    => [Expr ResolvedNames a] -> PositionOrExpr RawNames a -> Sem r (PositionOrExpr ResolvedNames a)
resolvePositionOrExpr _ (PositionOrExprExpr expr) = PositionOrExprExpr <$> resolveExpr expr
resolvePositionOrExpr exprs (PositionOrExprPosition info pos Unused)
    | pos < 1 = throw $ BadPositionalReference info pos
    | otherwise =
        case drop (pos - 1) exprs of
            expr:_ -> pure $ PositionOrExprPosition info pos expr
            [] -> throw $ BadPositionalReference info pos

resolveGroupingElement 
    :: (Members (ResolverEff a) r)
    => [Expr ResolvedNames a] -> GroupingElement RawNames a -> Sem r (GroupingElement ResolvedNames a)
resolveGroupingElement exprs (GroupingElementExpr info posOrExpr) =
    GroupingElementExpr info <$> resolvePositionOrExpr exprs posOrExpr
resolveGroupingElement _ (GroupingElementSet info exprs) =
    GroupingElementSet info <$> mapM resolveExpr exprs

resolveSelectGroup 
    :: (Members (ResolverEff a) r)
    => [Expr ResolvedNames a] -> SelectGroup RawNames a -> Sem r (SelectGroup ResolvedNames a)
resolveSelectGroup exprs SelectGroup{..} = do
    selectGroupGroupingElements' <- mapM (resolveGroupingElement exprs) selectGroupGroupingElements
    pure $ SelectGroup
        { selectGroupGroupingElements = selectGroupGroupingElements'
        , ..
        }

resolveSelectHaving 
    :: (Members (ResolverEff a) r)
    => SelectHaving RawNames a -> Sem r (SelectHaving ResolvedNames a)
resolveSelectHaving (SelectHaving info exprs) = SelectHaving info <$> mapM resolveExpr exprs

resolveSelectNamedWindow 
    :: (Members (ResolverEff a) r)
    => SelectNamedWindow RawNames a -> Sem r (SelectNamedWindow ResolvedNames a)
resolveSelectNamedWindow (SelectNamedWindow info windows) =
  SelectNamedWindow info <$> mapM resolveNamedWindowExpr windows
