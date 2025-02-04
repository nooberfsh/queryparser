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

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# Language TemplateHaskell #-}

module Database.Sql.Type.Scope where

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Unused
import Database.Sql.Type.Query

import Control.Monad.Identity

import           Data.Aeson
import qualified Data.HashMap.Strict as HMS
import           Data.HashMap.Strict (HashMap)

import Data.List (subsequences)
import Data.List.NonEmpty (NonEmpty, NonEmpty((:|)), (<|))
import Data.Hashable (Hashable)

import Test.QuickCheck

import Data.Data (Data)
import GHC.Generics (Generic)

import Polysemy
import qualified Polysemy.Error as PE
import qualified Polysemy.Writer as PW
import qualified Polysemy.State as PS
import qualified Polysemy.Reader as PR

-- | A ColumnSet records the table-bindings (if any) of columns.
--
-- Can be used to represent columns that are in ambient scope,
-- which can be referenced, based on arcane and dialect specific rules.
-- The fst component will be Nothing for collections of column
-- aliases bound in a containing select statement (which thus have no
-- corresponding Tablish), or for subqueries/lateral views with no table alias.
--
-- Can also be used to represent "what stars expand into".

type ColumnSet a = [(Maybe (RTableRef a), [RColumnRef a])]


data Bindings a = Bindings
    { boundCTEs :: [(TableAlias a, [RColumnRef a])]
    , boundColumns :: NonEmpty (ColumnSet a)
    }

emptyBindings :: Bindings a
emptyBindings = Bindings [] ([]:|[])

type BindForClause a = forall r s . Member (PR.Reader (ResolverInfo a)) r => Sem r s -> Sem r s

data SelectScope a = SelectScope
    { bindForHaving :: BindForClause a
    , bindForWhere :: BindForClause a
    , bindForOrder :: BindForClause a
    , bindForGroup :: BindForClause a
    , bindForNamedWindow :: BindForClause a
    }

type FromColumns a = ColumnSet a
type SelectionAliases a = [RColumnRef a]

data ResolverInfo a = ResolverInfo
    { onCTECollision :: forall x . (x -> x) -> (x -> x)
    , bindings :: Bindings a
    , lambdaScope :: [[LambdaParam a]]
    , selectScope :: FromColumns a -> SelectionAliases a -> SelectScope a
    , lcolumnsAreVisibleInLateralViews :: Bool
    }

mapBindings :: (Bindings a -> Bindings a) -> ResolverInfo a -> ResolverInfo a
mapBindings f ResolverInfo{..} = ResolverInfo{bindings = f bindings, ..}

bindNewScope :: Member (PR.Reader (ResolverInfo a)) r => Sem r s -> Sem r s
bindNewScope = PR.local (mapBindings $ \ (Bindings cte xs) -> Bindings cte ([]<|xs))

bindColumns :: Member (PR.Reader (ResolverInfo a)) r => ColumnSet a -> Sem r s -> Sem r s
bindColumns columns = PR.local (mapBindings $ \ (Bindings cte (x:|xs)) -> Bindings cte ((columns ++ x) :| xs))

bindLambdaParams :: Member (PR.Reader (ResolverInfo a)) r => [LambdaParam a] -> Sem r s -> Sem r s
bindLambdaParams params = PR.local (\ResolverInfo{..} -> ResolverInfo{lambdaScope = params:lambdaScope, ..})

bindFromColumns :: Member (PR.Reader (ResolverInfo a)) r => FromColumns a -> Sem r s -> Sem r s
bindFromColumns = bindColumns

bindAliasedColumns :: Member (PR.Reader (ResolverInfo a)) r => SelectionAliases a -> Sem r s -> Sem r s
bindAliasedColumns selectionAliases = bindColumns [(Nothing, selectionAliases)]

bindBothColumns :: Member (PR.Reader (ResolverInfo a)) r => FromColumns a -> SelectionAliases a -> Sem r s -> Sem r s
bindBothColumns fromColumns selectionAliases = bindColumns $ (Nothing, onlyNewAliases selectionAliases) : fromColumns
  where
    onlyNewAliases = filter $ \case
        RColumnAlias alias -> not $ inFromColumns alias
        RColumnRef _ -> False

    inFromColumns (ColumnAlias _ alias _) =
        let cols = map void (snd =<< fromColumns)
            aliases = [name | RColumnAlias (ColumnAlias _ name _) <- cols]
            fqcns = [name | RColumnRef (QColumnName _ _ name) <- cols]
         in alias `elem` aliases ++ fqcns

bindBothColumnsWithScope :: Member (PR.Reader (ResolverInfo a)) r => FromColumns a -> SelectionAliases a -> Sem r s -> Sem r s
bindBothColumnsWithScope fromColumns selectionAliases = do
    let newScope = [(Nothing, selectionAliases)] 
    PR.local (mapBindings $ \ (Bindings cte (x:|xs)) -> Bindings cte (newScope :| ((fromColumns ++ x):xs)))

data RawNames
deriving instance Data RawNames
instance Resolution RawNames where
    type TableRef RawNames = OQTableName
    type TableName RawNames = OQTableName
    type CreateTableName RawNames = OQTableName
    type DropTableName RawNames = OQTableName
    type SchemaName RawNames = OQSchemaName
    type ColumnRef RawNames = OQColumnName
    type NaturalColumns RawNames = Unused
    type UsingColumn RawNames = UQColumnName
    type StarReferents RawNames = Unused
    type PositionExpr RawNames = Unused
    type ComposedQueryColumns RawNames = Unused

data ResolvedNames
deriving instance Data ResolvedNames
newtype StarColumnNames a = StarColumnNames [RColumnRef a]
    deriving (Generic, Data, Eq, Ord, Show, Functor)

newtype ColumnAliasList a = ColumnAliasList [ColumnAlias a]
    deriving (Generic, Data, Eq, Ord, Show, Functor)

instance Resolution ResolvedNames where
    type TableRef ResolvedNames = RTableRef
    type TableName ResolvedNames = RTableName
    type CreateTableName ResolvedNames = FQTableName
    type DropTableName ResolvedNames = RDropTableName
    type SchemaName ResolvedNames = FQSchemaName
    type ColumnRef ResolvedNames = RColumnRef
    type NaturalColumns ResolvedNames = RNaturalColumns
    type UsingColumn ResolvedNames = RUsingColumn
    type StarReferents ResolvedNames = StarColumnNames
    type PositionExpr ResolvedNames = Expr ResolvedNames
    type ComposedQueryColumns ResolvedNames = ColumnAliasList

type ResolverEff a = 
    [ Catalog a
    , PE.Error (ResolutionError a)
    , PR.Reader (ResolverInfo a)
    , PW.Writer [Either (ResolutionError a) (ResolutionSuccess a)]
    , PS.State Integer -- column alias generation (counts down from -1, unlike parse phase)
    ] 

data SchemaMember = SchemaMember
    { tableType :: TableType
    , persistence :: Persistence ()
    , columnsList :: [UQColumnName ()]
    , viewQuery :: Maybe (Query ResolvedNames ())  -- this will always be Nothing for tables
    } deriving (Generic, Data, Eq, Ord, Show)

persistentTable :: [UQColumnName ()] -> SchemaMember
persistentTable cols = SchemaMember Table Persistent cols Nothing


type SchemaMap = HashMap (UQTableName ()) SchemaMember
type DatabaseMap = HashMap (UQSchemaName ()) SchemaMap
type CatalogMap = HashMap (DatabaseName ()) DatabaseMap
type Path = [UQSchemaName ()]
type CurrentDatabase = DatabaseName ()

data InMemoryCatalog = InMemoryCatalog
    { catalog :: CatalogMap
    , path :: Path
    , currentDb :: CurrentDatabase
    }
    deriving (Show, Eq)

inCurrentDb :: Applicative g => QSchemaName f a -> CurrentDatabase -> QSchemaName g a
inCurrentDb (QSchemaName sInfo _ schemaName schemaType) currentDb =
    let db = fmap (const sInfo) currentDb
     in QSchemaName sInfo (pure db) schemaName schemaType

inHeadOfPath :: Applicative g => QTableName f a -> Path -> CurrentDatabase -> QTableName g a
inHeadOfPath (QTableName tInfo _ tableName) path currentDb =
    let db = fmap (const tInfo) currentDb
        QSchemaName _ None schemaName schemaType = head path
        fqsn = QSchemaName tInfo (pure db) schemaName schemaType
     in QTableName tInfo (pure fqsn) tableName

data Catalog i m a where
    CatalogResolveSchemaName :: OQSchemaName i -> Catalog i m (FQSchemaName i)
    CatalogResolveTableName :: OQTableName i -> Catalog i m (RTableName i)
    CatalogHasTable :: UQTableName () -> Catalog i m Existence  -- | nb DoesNotExist does not imply that we can't resolve to this name (defaulting)
    CatalogResolveCreateSchemaName :: OQSchemaName i -> Catalog i m (FQSchemaName i)
    CatalogResolveCreateTableName :: OQTableName i -> Catalog i m (FQTableName i)
    CatalogResolveColumnName :: NonEmpty [(Maybe (RTableRef i), [RColumnRef i])] -> OQColumnName i -> Catalog i m (RColumnRef i)
    -- | apply schema changes TODO: add tests
    CatalogResolveCreateSchema :: FQSchemaName i -> Bool -> Catalog i m ()
    CatalogResolveCreateTable :: RTableName i -> Bool -> Catalog i m ()
    CatalogResolveDropTable :: OQTableName i -> Bool -> Catalog i m (RDropTableName i)

type CatalogEff a = 
    [ PE.Error (ResolutionError a) -- error
    , PW.Writer [Either (ResolutionError a) (ResolutionSuccess a)] -- warnings and successes
    ]

type CatalogInterpreter = forall i r a. (Members (CatalogEff i) r) => (Sem (Catalog i : r) a -> Sem (PS.State InMemoryCatalog : r) a, InMemoryCatalog)

data ResolutionError a
    = MissingDatabase (DatabaseName a)
    | MissingSchema (OQSchemaName a)
    | MissingTable (OQTableName a)
    | AmbiguousTable (OQTableName a)
    | MissingColumn (OQColumnName a)
    | AmbiguousColumn (OQColumnName a)
    | UnintroducedTable (OQTableName a)
    | UnexpectedTable (FQTableName a)
    | UnexpectedSchema (FQSchemaName a)
    | BadPositionalReference a Int
    | DeleteFromView (FQTableName a)
    | MissingFunctionExprForLateralView
        deriving (Eq, Show, Functor)

data ResolutionSuccess a
    = TableNameResolved (OQTableName a) (RTableName a)
    | TableNameDefaulted (OQTableName a) (RTableName a)
    | TableRefResolved (OQTableName a) (RTableRef a)
    | TableRefDefaulted (OQTableName a) (RTableRef a)
    | ColumnRefResolved (OQColumnName a) (RColumnRef a)
    | ColumnRefDefaulted (OQColumnName a) (RColumnRef a)
        deriving (Eq, Show, Functor)

isGuess :: ResolutionSuccess a -> Bool
isGuess (TableNameResolved _ _) = False
isGuess (TableNameDefaulted _ _) = True
isGuess (TableRefResolved _ _) = False
isGuess (TableRefDefaulted _ _) = True
isGuess (ColumnRefResolved _ _) = False
isGuess (ColumnRefDefaulted _ _) = True

isCertain :: ResolutionSuccess a -> Bool
isCertain = not . isGuess


data WithColumns r a = WithColumns
    { withColumnsValue :: r a
    , withColumnsColumns :: ColumnSet a
    }

data WithColumnsAndOrders r a = WithColumnsAndOrders (r a) (ColumnSet a) [Order ResolvedNames a]

-- R for "resolved"
data RTableRef a
    = RTableRef (FQTableName a) SchemaMember
    | RTableAlias (TableAlias a) [RColumnRef a]
      deriving (Generic, Data, Show, Eq, Ord, Functor, Foldable, Traversable)

getColumnList :: RTableRef a -> [RColumnRef a]
getColumnList (RTableRef fqtn SchemaMember{..}) = 
    let fqcns = map (\uqcn -> uqcn { columnNameInfo = tableNameInfo fqtn, columnNameTable = Identity fqtn }) columnsList
     in map RColumnRef fqcns
getColumnList (RTableAlias _ cols) = cols

resolvedTableHasName :: QTableName f a -> RTableRef a -> Bool
resolvedTableHasName (QTableName _ _ name) (RTableAlias (TableAlias _ name' _) _) = name' == name
resolvedTableHasName (QTableName _ _ name) (RTableRef (QTableName _ _ name') _) = name' == name

resolvedTableHasSchema :: QSchemaName f a -> RTableRef a -> Bool
resolvedTableHasSchema _ (RTableAlias _ _) = False
resolvedTableHasSchema (QSchemaName _ _ name schemaType) (RTableRef (QTableName _ (Identity (QSchemaName _ _ name' schemaType')) _) _) =
    name == name' && schemaType == schemaType'

resolvedTableHasDatabase :: DatabaseName a -> RTableRef a -> Bool
resolvedTableHasDatabase _ (RTableAlias _ _) = False
resolvedTableHasDatabase (DatabaseName _ name) (RTableRef (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ name')) _ _)) _) _) = name' == name


data RTableName a = RTableName (FQTableName a) SchemaMember
    deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

rTableNameToRTableRef :: RTableName a -> RTableRef a
rTableNameToRTableRef (RTableName fqtn sm) = RTableRef fqtn sm

data RDropTableName a
    = RDropExistingTableName (FQTableName a) SchemaMember
    | RDropMissingTableName (OQTableName a)
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


instance Arbitrary SchemaMember where
    arbitrary = do
        tableType <- arbitrary
        persistence <- arbitrary
        columnsList <- arbitrary
        viewQuery <- pure Nothing  -- TODO holding off til we have arbitrary queries
        pure SchemaMember{..}
    shrink (SchemaMember type_ persistence cols _) =
        [ SchemaMember type_' persistence' cols' Nothing |  -- TODO same
          (type_', persistence', cols') <- shrink (type_, persistence, cols) ]

shrinkHashMap :: (Eq k, Hashable k) => forall v.  HashMap k v -> [HashMap k v]
shrinkHashMap = map HMS.fromList . subsequences . HMS.toList

instance Arbitrary SchemaMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance Arbitrary DatabaseMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance Arbitrary CatalogMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance ToJSON a => ToJSON (RTableRef a) where
    toJSON (RTableRef fqtn _) = object
        [ "tag" .= String "RTableRef"
        , "fqtn" .= fqtn
        ]
    toJSON (RTableAlias alias _) = object
        [ "tag" .= String "RTableAlias"
        , "alias" .= alias
        ]

instance ToJSON a => ToJSON (RTableName a) where
    toJSON (RTableName fqtn _) = object
        [ "tag" .= String "RTableName"
        , "fqtn" .= fqtn
        ]

instance ToJSON a => ToJSON (RDropTableName a) where
    toJSON (RDropExistingTableName fqtn _) = object
        [ "tag" .= String "RDropExistingTableName"
        , "fqtn" .= fqtn
        ]
    toJSON (RDropMissingTableName oqtn) = object
        [ "tag" .= String "RDropMissingTableName"
        , "oqtn" .= oqtn
        ]

instance ToJSON a => ToJSON (StarColumnNames a) where
    toJSON (StarColumnNames cols) = object
        [ "tag" .= String "StarColumnNames"
        , "cols" .= cols
        ]

instance ToJSON a => ToJSON (ColumnAliasList a) where
    toJSON (ColumnAliasList cols) = object
        [ "tag" .= String "ColumnAliasList"
        , "cols" .= cols
        ]

makeSem ''Catalog
