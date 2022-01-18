{-# LANGUAGE OverloadedStrings #-}

module Database.Sql.Util.Scope.SchemaChange where

import           Test.HUnit
import           Database.Sql.Type as SQL
import           Database.Sql.Util.Scope (runResolverWErrorState)
import qualified Data.Text.Lazy as TL
import qualified Data.HashMap.Strict as HMS
import           Data.Proxy (Proxy(..))
import           Data.Functor.Identity
import           Control.Monad (void)

import qualified Database.Sql.Vertica.Parser as Vertica
import Database.Sql.Vertica.Type (Vertica, resolveVerticaStatement)

trans :: Either [ResolutionError a] (InMemoryCatalog, b, c) -> Either [ResolutionError ()] InMemoryCatalog
trans (Left errs) = Left $ void <$> errs
trans (Right (d, _, _)) = Right d

testVertica :: TL.Text -> InMemoryCatalog -> Either [ResolutionError ()] InMemoryCatalog -> Assertion
testVertica sql catalog expected = 
    case Vertica.parseAll sql of
        Right d -> 
            let res = runResolverWErrorState (resolveVerticaStatement d) (Proxy :: Proxy Vertica) (runInMemoryCatalog, catalog)
            
             in (trans res) @?= expected
        Left e -> assertFailure $ unlines
            [ "failed to parse:"
            , show sql
            , show e
            ]



testSchemaChange :: Test
testSchemaChange = test
    [ "Create Schema" ~: 
        [ testVertica "CREATE SCHEMA web;" (mkCatalogSchema ["web"]) (Left [UnexpectedSchema $ mkFQSchemaName "web" ])
        , testVertica "CREATE SCHEMA web;" (mkCatalogSchema ["x"]) (Right (mkCatalogSchema ["x", "web"]))
        , testVertica "CREATE SCHEMA IF NOT EXISTS web;" (mkCatalogSchema ["web"]) (Right (mkCatalogSchema ["web"]))
        , testVertica "CREATE SCHEMA IF NOT EXISTS web;" (mkCatalogSchema ["x"]) (Right (mkCatalogSchema ["x", "web"]))
        , testVertica "CREATE SCHEMA x.web;" (mkCatalogSchema ["web"]) (Left [MissingDatabase $ mkDatabaseName "x" ])
        , testVertica "CREATE SCHEMA default_db.web;" (mkCatalogSchema ["x"]) (Right (mkCatalogSchema ["x", "web"]))
        ]

    , "Create Table" ~:
        [ testVertica "CREATE TABLE t1 LIKE t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0", "t1"]))
        , testVertica "CREATE TABLE t1 LIKE t0" (mkCatalogTable ["t0", "t1"]) (Left [UnexpectedTable $ mkFQTableName "t1"])
        , testVertica "CREATE TABLE IF NOT EXISTS t1 LIKE t0" (mkCatalogTable ["t0", "t1"]) (Right (mkCatalogTable ["t0", "t1"]))
        , testVertica "CREATE TABLE IF NOT EXISTS t1 LIKE t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0", "t1"]))
        , testVertica "CREATE TABLE public.t1 LIKE t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0", "t1"]))
        , testVertica "CREATE TABLE public1.t1 LIKE t0" (mkCatalogTable ["t0"]) (Left [MissingSchema $ fqsnToOqsn (mkFQSchemaName "public1")])
        , testVertica "CREATE TABLE default_db.public.t1 LIKE t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0", "t1"]))
        , testVertica "CREATE TABLE default_db1.public.t1 LIKE t0" (mkCatalogTable ["t0"]) (Left [MissingDatabase (mkDatabaseName "default_db1")])
        ]
    
    , "Drop Table" ~:
        [ testVertica "DROP TABLE t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable []))
        , testVertica "DROP TABLE t1" (mkCatalogTable ["t0"]) (Left [MissingTable $ mkOQTableName "t1"])
        , testVertica "DROP TABLE IF EXISTS t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable []))
        , testVertica "DROP TABLE IF EXISTS t1" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0"]))
        , testVertica "DROP TABLE public.t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable []))
        , testVertica "DROP TABLE IF EXISTS public1.t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0"]))
        , testVertica "DROP TABLE public1.t1" (mkCatalogTable ["t0"]) (Left [MissingTable $ mkOQTableName' "public1" "t1"])
        , testVertica "DROP TABLE default_db.public.t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable []))
        , testVertica "DROP TABLE IF EXISTS default_db1.public.t0" (mkCatalogTable ["t0"]) (Right (mkCatalogTable ["t0"]))
        , testVertica "DROP TABLE default_db1.public.t1" (mkCatalogTable ["t0"]) (Left [MissingTable $ mkOQTableName'' "default_db1" "public" "t1"])
        ]
    ]

defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

publicSchema :: UQSchemaName ()
publicSchema = mkNormalSchema "public" ()

mkDatabaseName :: TL.Text -> DatabaseName ()
mkDatabaseName = DatabaseName () 

mkFQSchemaName :: TL.Text -> FQSchemaName ()
mkFQSchemaName name = QSchemaName () (Identity defaultDatabase) name  NormalSchema

mkOQSchemaName :: TL.Text -> OQSchemaName ()
mkOQSchemaName name = QSchemaName () (Just defaultDatabase) name  NormalSchema

mkOQSchemaName' :: TL.Text -> TL.Text -> OQSchemaName ()
mkOQSchemaName' db name = QSchemaName () (Just $ mkDatabaseName db) name  NormalSchema

mkFQTableName :: TL.Text -> FQTableName ()
mkFQTableName = QTableName () (Identity (mkFQSchemaName "public"))

mkOQTableName :: TL.Text -> OQTableName ()
mkOQTableName = QTableName () Nothing

mkOQTableName' :: TL.Text -> TL.Text -> OQTableName ()
mkOQTableName' schema = QTableName () (Just $ mkOQSchemaName schema)

mkOQTableName'' :: TL.Text -> TL.Text -> TL.Text -> OQTableName ()
mkOQTableName'' db schema = QTableName () (Just $ mkOQSchemaName' db schema)

mkCatalogSchema :: [TL.Text] -> InMemoryCatalog
mkCatalogSchema schemas = InMemoryCatalog
    ( HMS.singleton defaultDatabase $ HMS.fromList
        (map (\n -> (mkNormalSchema n (), HMS.empty)) schemas)
    )
    [publicSchema]
    defaultDatabase

mkCatalogTable :: [TL.Text] -> InMemoryCatalog
mkCatalogTable tables = InMemoryCatalog
    ( HMS.singleton defaultDatabase $ HMS.fromList
        [ ( publicSchema
          , HMS.fromList
            (map (\n -> (QTableName () None n , persistentTable [])) tables)
          )
        ]
    )
    [publicSchema]
    defaultDatabase

tests :: Test
tests = test [testSchemaChange] 

