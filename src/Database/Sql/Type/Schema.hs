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

{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# Language NoMonoLocalBinds #-}

module Database.Sql.Type.Schema where

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Scope

import Control.Applicative (liftA2, liftA3)
import Data.Functor.Identity
import Control.Monad (void)

import qualified Data.HashMap.Strict as HMS

import Data.Maybe (mapMaybe, maybeToList)
import Data.List.NonEmpty (NonEmpty((:|)))

import Polysemy
import Polysemy.Error
import Polysemy.Writer
import Polysemy.State

and3 :: Bool -> Bool -> Bool -> Bool
and3 x y z = x && y && z

overWithColumns :: (r a -> s a) -> WithColumns r a -> WithColumns s a
overWithColumns f (WithColumns r cs) = WithColumns (f r) cs

resolvedColumnHasName :: QColumnName f a -> RColumnRef a -> Bool
resolvedColumnHasName (QColumnName _ _ name) (RColumnAlias (ColumnAlias _ name' _)) = name' == name
resolvedColumnHasName (QColumnName _ _ name) (RColumnRef (QColumnName _ _ name')) = name' == name

runInMemoryCatalog 
    :: forall a r s. (Members (CatalogEff a) r)
    => Sem (Catalog a : r) s -> Sem (State InMemoryCatalog : r) s
runInMemoryCatalog = reinterpret $ \case
    -- TODO session schemas should have the name set to the session ID
    CatalogResolveSchemaName oqsn -> do
        InMemoryCatalog {..} <- get
        pure $ catalogResolveSchemaNameHelper oqsn currentDb

    CatalogResolveTableName oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) ->
        catalogResolveTableNameHelper oqtn

    CatalogResolveTableName (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) -> do
        InMemoryCatalog {..} <- get
        catalogResolveTableNameHelper $ QTableName tInfo (Just $ inCurrentDb oqsn currentDb) tableName

    CatalogResolveTableName oqtn@(QTableName tInfo Nothing tableName) -> do
        InMemoryCatalog {..} <- get
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            rtn:_ -> do
                tell [Right $ TableNameResolved oqtn rtn]
                pure rtn
            [] -> throw $ MissingTable oqtn

    CatalogHasTable tableName -> do
        InMemoryCatalog {..} <- get
        let getTableFromSchema uqsn = do
                database <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn database
                pure $ HMS.member tableName schema
         in case any id $ mapMaybe getTableFromSchema path of
            False -> return DoesNotExist
            True -> return Exists

    CatalogResolveCreateSchemaName oqsn -> do
        InMemoryCatalog {..} <- get
        case schemaNameType oqsn of
            NormalSchema -> pure $ catalogResolveSchemaNameHelper oqsn currentDb
            SessionSchema -> error "can't create the session schema"


    CatalogResolveCreateTableName name -> do
        InMemoryCatalog {..} <- get
        ~(QTableName tInfo (Just (QSchemaName sInfo (Just db) schemaName schemaType)) tableName) <-
                case name of
                    oqtn@(QTableName _ Nothing _) -> pure $ inHeadOfPath oqtn path currentDb
                    QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName -> pure $ QTableName tInfo (pure $ inCurrentDb oqsn currentDb) tableName
                    _ -> pure name

        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName 
        pure fqtn

    CatalogResolveColumnName (boundColumns:|boundColumnsRest) oqcn -> do
        result <- catalogResolveColumnName' (boundColumns:boundColumnsRest) oqcn
        let tbl = columnNameTable oqcn
        case (result, tbl) of
            (Nothing, Nothing) -> throw $ MissingColumn oqcn
            (Nothing, Just oqtn) -> throw $ UnintroducedTable oqtn
            (Just x, _) -> pure x

    CatalogResolveCreateSchema (QSchemaName _ _ _ SessionSchema) _ -> error "can't create the session schema"

    CatalogResolveCreateSchema fqsn@(QSchemaName _ (Identity db) schemaName NormalSchema) createIfNotExists -> do
        InMemoryCatalog {..} <- get
        case HMS.lookup (void db) catalog of
            Nothing -> throw $ MissingDatabase db
            Just database -> 
                if HMS.member (QSchemaName () None schemaName NormalSchema) database then
                    if createIfNotExists then pure () else throw $ UnexpectedSchema fqsn
                else do
                    let fqsn' = QSchemaName () None schemaName NormalSchema
                        newDb = HMS.insert fqsn' HMS.empty database
                        newCatalog = HMS.insert (void db) newDb catalog
                    put InMemoryCatalog {catalog = newCatalog, ..}
                    pure ()

    CatalogResolveCreateTable (RTableName fqtn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity dbName) _ _)) _) schemaMember) createIfNotExists -> do
        InMemoryCatalog {..} <- get
        let uqtn = void $ fqtnToUqtn fqtn
            uqsn = void $ fqsnToUqsn fqsn
        db <- getDatabase dbName catalog
        schema <- getSchema (fqsnToOqsn fqsn) currentDb catalog
        if HMS.member uqtn schema then
            if createIfNotExists then pure () else throw $ UnexpectedTable fqtn
        else do
            let 
                newSchema = HMS.insert uqtn schemaMember schema
                newDb = HMS.insert uqsn newSchema db
                newCatalog = HMS.insert (void dbName) newDb catalog
            put InMemoryCatalog {catalog = newCatalog, ..}
            pure ()

    CatalogResolveDropTable oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) ifExists ->
        catalogResolveDropTableHelper oqtn ifExists

    CatalogResolveDropTable (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) ifExists -> do
        InMemoryCatalog {..} <- get
        catalogResolveDropTableHelper (QTableName tInfo (Just $ inCurrentDb oqsn currentDb) tableName) ifExists

    CatalogResolveDropTable oqtn@(QTableName tInfo Nothing tableName) ifExists -> do
        InMemoryCatalog {..} <- get
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            (RTableName fqtn _):_ -> catalogResolveDropTableHelper (fqtnToOqtn fqtn) ifExists
            [] -> if ifExists then pure $ RDropMissingTableName oqtn else throw $ MissingTable oqtn

  where
    catalogResolveTableNameHelper 
        :: (Members (CatalogEff a) r)
        => OQTableName a -> Sem (State InMemoryCatalog : r) (RTableName a)
    catalogResolveTableNameHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        InMemoryCatalog {..} <- get
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName
        case HMS.lookup (void db) catalog of
            Nothing -> throw $ MissingDatabase db
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> throw $ MissingSchema oqsn
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> throw $ MissingTable oqtn
                            Just table -> do
                                let rtn = RTableName fqtn table
                                tell [Right $ TableNameResolved oqtn rtn]
                                pure rtn

    catalogResolveTableNameHelper _ = error "only call catalogResolveTableNameHelper with fully qualified table name"

    catalogResolveColumnName'
        :: (Members (CatalogEff a) r)
        => [[(Maybe (RTableRef a), [RColumnRef a])]] -> OQColumnName a -> Sem (State InMemoryCatalog : r) (Maybe (RColumnRef a))
    catalogResolveColumnName' [] _ = pure Nothing
    catalogResolveColumnName' (x:xs) oqsn = do
        res <- catalogResolveColumnNameHelper x oqsn
        case res of
            Nothing -> catalogResolveColumnName' xs oqsn
            Just d -> pure $ Just d

    catalogResolveColumnNameHelper 
        :: (Members (CatalogEff a) r)
        => [(Maybe (RTableRef a), [RColumnRef a])] -> OQColumnName a -> Sem (State InMemoryCatalog : r) (Maybe (RColumnRef a))
    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName _ (Just oqsn@(QSchemaName _ (Just db) _ _)) _)) _) = do
        case filter (maybe False (liftA3 and3 (resolvedTableHasDatabase db) (resolvedTableHasSchema oqsn) (resolvedTableHasName oqtn)) . fst) boundColumns of
            [] -> pure Nothing
            _:_:_ -> throw $ AmbiguousTable oqtn
            [(_, columns)] ->
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> throw $ MissingColumn oqcn
                    [c] -> do
                        let c' = fmap (const cInfo) c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure $ Just c'
                    _ -> throw $ AmbiguousColumn oqcn

    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName _ (Just oqsn@(QSchemaName _ Nothing _ _)) _)) _) = do
        case filter (maybe False (liftA2 (&&) (resolvedTableHasSchema oqsn) (resolvedTableHasName oqtn)) . fst) boundColumns of
            [] -> pure Nothing
            _:_:_ -> throw $ AmbiguousTable oqtn
            [(_, columns)] ->
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> throw $ MissingColumn oqcn
                    [c] -> do
                        let c' = fmap (const cInfo) c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure $ Just c'
                    _ -> throw $ AmbiguousColumn oqcn

    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo Nothing table)) column) = do
        let setInfo :: Functor f => f a -> f a
            setInfo = fmap (const cInfo)

        case [ (t, cs) | (mt, cs) <- boundColumns, t <- maybeToList mt, resolvedTableHasName oqtn t ] of
            [] -> pure Nothing
            [(table', columns)] -> do
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> case table' of
                        RTableAlias _ _ -> throw $ MissingColumn oqcn
                        RTableRef fqtn@(QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) schema schemaType)) _) _ -> do
                            let c = RColumnRef $ QColumnName cInfo (pure $ setInfo fqtn) column
                            tell [ Left $ MissingColumn $ QColumnName cInfo (Just $ QTableName tInfo (Just $ QSchemaName cInfo (Just $ DatabaseName cInfo db) schema schemaType) table) column
                                 , Right $ ColumnRefResolved oqcn c]
                            pure $ Just c
                    [c] -> do
                        let c' = setInfo c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure $ Just c'
                    _ -> throw $ AmbiguousColumn oqcn
            tables -> do
                tell [Left $ AmbiguousTable oqtn]
                case filter (resolvedColumnHasName oqcn) $ snd =<< tables of
                    [] -> throw $ MissingColumn oqcn
                    [c] -> do
                        let c' = setInfo c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure $ Just c'
                    _ -> throw $ AmbiguousColumn oqcn

    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo Nothing _) = do
        let columns = snd =<< boundColumns
        case filter (resolvedColumnHasName oqcn) columns of
            [] -> pure Nothing
            [c] -> do
                let c' = fmap (const cInfo) c
                tell [Right $ ColumnRefResolved oqcn c']
                pure $ Just c'
            _ -> throw $ AmbiguousColumn oqcn

    catalogResolveDropTableHelper 
        :: (Members (CatalogEff a) r)
        => OQTableName a -> Bool -> Sem (State InMemoryCatalog : r) (RDropTableName a)
    catalogResolveDropTableHelper oqtn@(QTableName tInfo (Just (QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) ifExists = do
        InMemoryCatalog {..} <- get
        let missingTbl = RDropMissingTableName oqtn
            fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            uqsn = void $ fqsnToUqsn fqsn
            fqtn = QTableName tInfo (pure fqsn) tableName
            uqtn = void $ fqtnToUqtn fqtn
        case HMS.lookup (void db) catalog of
            Nothing -> if ifExists then pure missingTbl else throw $ MissingTable oqtn
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> if ifExists then pure missingTbl else throw $ MissingTable oqtn
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> if ifExists then pure missingTbl else throw $ MissingTable oqtn
                            Just table -> do
                                let 
                                    newSchema = HMS.delete uqtn schema
                                    newDb = HMS.insert uqsn newSchema database
                                    newCatalog = HMS.insert (void db) newDb catalog
                                put InMemoryCatalog {catalog = newCatalog, ..}
                                pure $ RDropExistingTableName fqtn table

    catalogResolveDropTableHelper _ _ = error "only call catalogResolveDropTableHelper with fully qualified table name"

    catalogResolveSchemaNameHelper (QSchemaName sInfo (Just db) schemaName schemaType) _ = QSchemaName sInfo (pure db) schemaName schemaType
    catalogResolveSchemaNameHelper oqsn@(QSchemaName _ Nothing _ _) currentDb = inCurrentDb oqsn currentDb


defaultSchemaMember :: SchemaMember
defaultSchemaMember = SchemaMember{..}
  where
    tableType = Table
    persistence = Persistent
    columnsList = []
    viewQuery = Nothing

unknownDatabase :: a -> DatabaseName a
unknownDatabase info = DatabaseName info "<unknown>"

unknownSchema :: a -> FQSchemaName a
unknownSchema info = QSchemaName info (pure $ unknownDatabase info) "<unknown>" NormalSchema

unknownTable :: a -> FQTableName a
unknownTable info = QTableName info (pure $ unknownSchema info) "<unknown>"


runInMemoryDefaultingCatalog 
    :: forall a r s. (Members (CatalogEff a) r)
    => Sem (Catalog a : r) s -> Sem (State InMemoryCatalog : r) s
runInMemoryDefaultingCatalog = reinterpret $ \case
    -- TODO session schemas should have the name set to the session ID
    CatalogResolveSchemaName oqsn -> do
        InMemoryCatalog {..} <- get
        pure $ catalogResolveSchemaNameHelper oqsn currentDb

    CatalogResolveTableName oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) -> catalogResolveTableNameHelper oqtn

    CatalogResolveTableName (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) -> do 
        InMemoryCatalog {..} <- get
        catalogResolveTableNameHelper $ QTableName tInfo (Just $ inCurrentDb oqsn currentDb) tableName

    CatalogResolveTableName oqtn@(QTableName tInfo Nothing tableName) -> do
        InMemoryCatalog {..} <- get
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            rtn:_ -> do
                tell [Right $ TableNameResolved oqtn rtn]
                pure rtn
            [] -> do
                let rtn = RTableName (inHeadOfPath oqtn path currentDb) $ persistentTable []
                tell [ Left $ MissingTable oqtn
                     , Right $ TableNameDefaulted oqtn rtn
                     ]
                pure rtn

    CatalogHasTable tableName -> do
        InMemoryCatalog {..} <- get
        let getTableFromSchema uqsn = do
                database <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn database
                pure $ HMS.member tableName schema
         in case any id $ mapMaybe getTableFromSchema path of
            False -> return DoesNotExist
            True -> return Exists

    CatalogResolveCreateSchemaName oqsn -> do
        InMemoryCatalog {..} <- get
        case schemaNameType oqsn of
            NormalSchema -> pure $ catalogResolveSchemaNameHelper oqsn currentDb
            SessionSchema -> error "can't create the session schema"


    CatalogResolveCreateTableName name -> do
        InMemoryCatalog {..} <- get
        ~(QTableName tInfo (Just (QSchemaName sInfo (Just db) schemaName schemaType)) tableName) <-
                case name of
                    oqtn@(QTableName _ Nothing _) -> pure $ inHeadOfPath oqtn path currentDb
                    QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName -> pure $ QTableName tInfo (pure $ inCurrentDb oqsn currentDb) tableName
                    _ -> pure name

        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName 
        pure fqtn

    CatalogResolveColumnName (boundColumns:|boundColumnsRest) oqcn -> 
        -- TODO: resolve column in each boundColumns
        catalogResolveColumnNameHelper (concat (boundColumns:boundColumnsRest)) oqcn

    CatalogResolveCreateSchema (QSchemaName _ _ _ SessionSchema) _ -> error "can't create the session schema"

    CatalogResolveCreateSchema fqsn@(QSchemaName _ (Identity db) schemaName NormalSchema) createIfNotExists -> do
        InMemoryCatalog {..} <- get
        case HMS.lookup (void db) catalog of
            Nothing -> tell [Left $ MissingDatabase db]
            Just database -> 
                if HMS.member (QSchemaName () None schemaName NormalSchema) database then
                    if createIfNotExists then pure () else tell [Left $ UnexpectedSchema fqsn]
                else do
                    let fqsn' = QSchemaName () None schemaName NormalSchema
                        newDb = HMS.insert fqsn' HMS.empty database
                        newCatalog = HMS.insert (void db) newDb catalog
                    put InMemoryCatalog {catalog = newCatalog, ..}
                    pure ()

    CatalogResolveCreateTable (RTableName fqtn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity dbName) _ _)) _) schemaMember) createIfNotExists -> do
        InMemoryCatalog {..} <- get
        let uqtn = void $ fqtnToUqtn fqtn
            uqsn = void $ fqsnToUqsn fqsn
        case HMS.lookup (void dbName) catalog of
            Nothing -> tell [Left $ MissingDatabase dbName]
            Just database -> 
                case HMS.lookup uqsn database of
                    Nothing -> tell [Left $ MissingSchema (fqsnToOqsn fqsn)]
                    Just schema ->
                        if HMS.member uqtn schema then
                            if createIfNotExists then pure () else tell [Left $ UnexpectedTable fqtn]
                        else do
                            let 
                                newSchema = HMS.insert uqtn schemaMember schema
                                newDb = HMS.insert uqsn newSchema database
                                newCatalog = HMS.insert (void dbName) newDb catalog
                            put InMemoryCatalog {catalog = newCatalog, ..}
                            pure ()

    CatalogResolveDropTable oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) ifExists ->
        catalogResolveDropTableHelper oqtn ifExists

    CatalogResolveDropTable (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) ifExists -> do
        InMemoryCatalog {..} <- get
        catalogResolveDropTableHelper (QTableName tInfo (Just $ inCurrentDb oqsn currentDb) tableName) ifExists

    CatalogResolveDropTable oqtn@(QTableName tInfo Nothing tableName) ifExists -> do
        InMemoryCatalog {..} <- get
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            (RTableName fqtn _):_ -> catalogResolveDropTableHelper (fqtnToOqtn fqtn) ifExists
            [] -> 
                if ifExists then pure $ RDropMissingTableName oqtn 
                else do 
                    tell [Left $ MissingTable oqtn]
                    pure $ RDropMissingTableName oqtn
                

  where
    catalogResolveTableNameHelper 
        :: (Members (CatalogEff a) r)
        => OQTableName a -> Sem (State InMemoryCatalog : r) (RTableName a)
    catalogResolveTableNameHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        InMemoryCatalog {..} <- get
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName
            default' = RTableName fqtn (persistentTable [])
            missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
            missingT = Left $ MissingTable oqtn
            tableNameResolved = Right $ TableNameResolved oqtn default'
        case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS, missingT, tableNameResolved] >> pure default'
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> tell [missingS, missingT, tableNameResolved] >> pure default'
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> tell [missingT, tableNameResolved] >> pure default'
                            Just table -> do
                                let rtn = RTableName fqtn table
                                tell [Right $ TableNameResolved oqtn rtn]
                                pure rtn

    catalogResolveTableNameHelper _ = error "only call catalogResolveTableNameHelper with fully qualified table name"

    catalogResolveColumnNameHelper 
        :: (Members (CatalogEff a) r)
        => [(Maybe (RTableRef a), [RColumnRef a])] -> OQColumnName a -> Sem (State InMemoryCatalog : r) (RColumnRef a)
    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db) schema schemaType)) table)) column) = do
        case filter (maybe False (liftA3 and3 (resolvedTableHasDatabase db) (resolvedTableHasSchema oqsn) (resolvedTableHasName oqtn)) . fst) boundColumns of
            [] -> tell [Left $ UnintroducedTable oqtn]
            _:_:_ -> tell [Left $ AmbiguousTable oqtn]
            [(_, columns)] ->
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> tell [Left $ MissingColumn oqcn]
                    [_] -> pure ()
                    _ -> tell [Left $ AmbiguousColumn oqcn]
        let columnRef = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ QSchemaName sInfo (pure db) schema schemaType) table) column
        tell [Right $ ColumnRefResolved oqcn columnRef]
        pure columnRef

    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo Nothing schema schemaType)) table)) column) = do
        InMemoryCatalog {..} <- get
        let filtered = filter (maybe False (liftA2 (&&) (resolvedTableHasSchema oqsn) (resolvedTableHasName oqtn)) . fst) boundColumns
            fqtnDefault = QTableName tInfo (Identity $ inCurrentDb oqsn currentDb) table
        fqtn <- case filtered of
            [] -> tell [Left $ UnintroducedTable oqtn] >> pure fqtnDefault
            _:_:_ -> tell [Left $ AmbiguousTable oqtn] >> pure fqtnDefault
            [(table', columns)] -> do
                let Just (RTableRef (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) _ _)) _) _) = table' -- this pattern match shouldn't fail:
                     -- the `maybe False` prevents Nothings, and the `resolvedTableHasSchema` prevents RTableAliases
                    oqcnKnownDb = QColumnName cInfo (Just $ QTableName tInfo (Just $ QSchemaName sInfo (Just $ DatabaseName cInfo db) schema schemaType) table) column
                    fqtnKnownDb = QTableName tInfo (Identity $ QSchemaName sInfo (Identity $ DatabaseName cInfo db) schema schemaType) table
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> tell [Left $ MissingColumn oqcnKnownDb] >> pure fqtnKnownDb
                    [_] -> pure fqtnKnownDb
                    _ -> tell [Left $ AmbiguousColumn oqcnKnownDb] >> pure fqtnKnownDb

        let columnRef = RColumnRef $ QColumnName cInfo (pure fqtn) column
        tell [Right $ ColumnRefResolved oqcn columnRef]
        pure columnRef

    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo Nothing table)) column) = do
        let setInfo :: Functor f => f a -> f a
            setInfo = fmap (const cInfo)

        case [ (t, cs) | (mt, cs) <- boundColumns, t <- maybeToList mt, resolvedTableHasName oqtn t ] of
            [] -> do
                let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ unknownSchema tInfo) table) column
                tell [ Left $ UnintroducedTable oqtn
                     , Right $ ColumnRefDefaulted oqcn c
                     ]
                pure c
            [(table', columns)] -> do
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> case table' of
                        RTableAlias _ _-> do
                            let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ unknownSchema tInfo) table) column
                            tell [ Left $ MissingColumn oqcn
                                 , Right $ ColumnRefDefaulted oqcn c
                                 ]
                            pure c
                        RTableRef fqtn@(QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) schema schemaType)) _) _ -> do
                            let c = RColumnRef $ QColumnName cInfo (pure $ setInfo fqtn) column
                            tell [ Left $ MissingColumn $ QColumnName cInfo (Just $ QTableName tInfo (Just $ QSchemaName cInfo (Just $ DatabaseName cInfo db) schema schemaType) table) column
                                 , Right $ ColumnRefResolved oqcn c]
                            pure c
                    c:rest -> do
                        let c' = setInfo c
                        if (null rest)
                            then tell [Right $ ColumnRefResolved oqcn c']
                            else tell [ Left $ AmbiguousColumn oqcn
                                      , Right $ ColumnRefDefaulted oqcn c'
                                      ]
                        pure c'
            tables -> do
                tell [Left $ AmbiguousTable oqtn]
                case filter (resolvedColumnHasName oqcn) $ snd =<< tables of
                    [] -> do
                        let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ unknownSchema tInfo) table) column
                        tell [ Left $ MissingColumn oqcn
                             , Right $ ColumnRefDefaulted oqcn c
                             ]
                        pure c
                    c:rest -> do
                        let c' = setInfo c
                        if (null rest)
                            then tell [Right $ ColumnRefResolved oqcn c']
                            else tell [ Left $ AmbiguousColumn oqcn
                                      , Right $ ColumnRefDefaulted oqcn c'
                                      ]
                        pure c'

    catalogResolveColumnNameHelper boundColumns oqcn@(QColumnName cInfo Nothing column) = do
        let columns = snd =<< boundColumns
        case filter (resolvedColumnHasName oqcn) columns of
            [] -> do
                let table =
                        case boundColumns of
                            [(Just (RTableRef t _), _)] -> t
                            _ -> unknownTable cInfo
                    c = RColumnRef $ QColumnName cInfo (pure table) column
                tell [ Left $ MissingColumn oqcn
                     , Right $ ColumnRefDefaulted oqcn c
                     ]
                pure c
            c:rest -> do
                let c' = fmap (const cInfo) c
                if (null rest)
                    then tell [ Right $ ColumnRefResolved oqcn c' ]
                    else tell [ Left $ AmbiguousColumn oqcn
                              , Right $ ColumnRefDefaulted oqcn c'
                              ]
                pure c'


    catalogResolveDropTableHelper 
        :: (Members (CatalogEff a) r)
        => OQTableName a -> Bool -> Sem (State InMemoryCatalog : r) (RDropTableName a)
    catalogResolveDropTableHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) ifExists = do
        InMemoryCatalog {..} <- get
        let missingTbl = RDropMissingTableName oqtn
            fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            uqsn = void $ fqsnToUqsn fqsn
            fqtn = QTableName tInfo (pure fqsn) tableName
            uqtn = void $ fqtnToUqtn fqtn
        case HMS.lookup (void db) catalog of
            Nothing -> if ifExists then pure missingTbl else tell [Left $ MissingDatabase db] >> pure missingTbl
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> if ifExists then pure missingTbl else tell [Left $ MissingSchema oqsn] >> pure missingTbl
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> if ifExists then pure missingTbl else tell [Left $ MissingTable oqtn] >> pure missingTbl
                            Just table -> do
                                let 
                                    newSchema = HMS.delete uqtn schema
                                    newDb = HMS.insert uqsn newSchema database
                                    newCatalog = HMS.insert (void db) newDb catalog
                                put InMemoryCatalog {catalog = newCatalog, ..}
                                pure $ RDropExistingTableName fqtn table

    catalogResolveDropTableHelper _ _ = error "only call catalogResolveDropTableHelper with fully qualified table name"

    catalogResolveSchemaNameHelper (QSchemaName sInfo (Just db) schemaName schemaType) _ = QSchemaName sInfo (pure db) schemaName schemaType
    catalogResolveSchemaNameHelper oqsn@(QSchemaName _ Nothing _ _) currentDb = inCurrentDb oqsn currentDb


getDatabase :: Members (CatalogEff a) r => DatabaseName a -> CatalogMap -> Sem r DatabaseMap
getDatabase name catalog = do
    case HMS.lookup (void name) catalog of
        Nothing -> throw $ MissingDatabase name
        Just database -> pure database

getSchema :: Members (CatalogEff a) r => OQSchemaName a -> CurrentDatabase -> CatalogMap -> Sem r SchemaMap
getSchema oqsn@QSchemaName {schemaNameDatabase = Just dbName, ..} _ catalog = do
    db <- getDatabase dbName catalog
    let uqsn = QSchemaName {schemaNameInfo = (), schemaNameDatabase = None, ..}
    case HMS.lookup uqsn db of
        Nothing -> throw $ MissingSchema oqsn
        Just schema -> pure schema

getSchema QSchemaName {schemaNameDatabase = Nothing, ..} dbName catalog = do
    let dbName' = fmap (const schemaNameInfo ) dbName
    let uqsn = QSchemaName {schemaNameDatabase = Just dbName', ..}
    getSchema uqsn dbName catalog

