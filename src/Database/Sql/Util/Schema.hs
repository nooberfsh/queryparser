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

module Database.Sql.Util.Schema where

import Data.List.NonEmpty (NonEmpty (..))

import Database.Sql.Type.Names
import Database.Sql.Type.Scope

import qualified Database.Sql.Type as AST

import qualified Data.Foldable as F

import Control.Monad.Reader
import Data.Functor.Identity


data SchemaChange
    = AddColumn (FQColumnName ())
    | DropColumn (FQColumnName ())
    | DropTable (FQTableName ())
    | DropView (FQTableName ())
    | DropSchema (FQSchemaName ())
    | CreateDatabase (DatabaseName ()) DatabaseMap


data SchemaChangeError
    = DatabaseMissing (DatabaseName ())
    | SchemaMissing (FQSchemaName ())
    | TableMissing (FQTableName ())
    | ColumnMissing (FQColumnName ())
    | DatabaseCollision (DatabaseName ())
    | SchemaCollision (FQSchemaName ())
    | TableCollision (FQTableName ())
    | ColumnCollision (FQColumnName ())
    | UnsupportedColumnChange (FQTableName ())
    deriving (Eq, Show)


class HasSchemaChange q where
    getSchemaChange :: q -> [SchemaChange]


instance HasSchemaChange (AST.Statement d AST.ResolvedNames a) where
    getSchemaChange (AST.QueryStmt _) = []
    getSchemaChange (AST.InsertStmt _) = []
    getSchemaChange (AST.UpdateStmt _) = []
    getSchemaChange (AST.CreateTableStmt _) = []

    getSchemaChange (AST.AlterTableStmt stmt) = getSchemaChange stmt


    getSchemaChange (AST.DeleteStmt _) = []
    getSchemaChange (AST.TruncateStmt _) = []
    getSchemaChange (AST.DropTableStmt AST.DropTable{dropTableNames = tables}) =
      F.foldMap (\case
                   AST.RDropExistingTableName fqtn _ -> [DropTable $ void fqtn]
                   AST.RDropMissingTableName _ -> []
               ) tables
    getSchemaChange (AST.CreateViewStmt _) = []
    getSchemaChange (AST.DropViewStmt AST.DropView{dropViewName = AST.RDropExistingTableName fqvn _}) = [DropView $ void fqvn]
    getSchemaChange (AST.DropViewStmt AST.DropView{dropViewName = AST.RDropMissingTableName _}) = []
    getSchemaChange (AST.CreateSchemaStmt _) = []
    getSchemaChange (AST.GrantStmt _) = []
    getSchemaChange (AST.RevokeStmt _) = []
    getSchemaChange (AST.BeginStmt _) = []
    getSchemaChange (AST.CommitStmt _) = []
    getSchemaChange (AST.RollbackStmt _) = []
    getSchemaChange (AST.ExplainStmt _ _) = []
    getSchemaChange (AST.EmptyStmt _) = []


instance HasSchemaChange (AST.AlterTable AST.ResolvedNames a) where
    getSchemaChange (AST.AlterTableRenameTable _ (AST.RTableName from _) _) =
        [ DropTable (void from)
        ]
    getSchemaChange (AST.AlterTableRenameColumn _ (AST.RTableName table _) from to) =
        let sameCol :: AST.UQColumnName a -> AST.UQColumnName a -> Bool
            sameCol (AST.QColumnName _ _ fromName) (AST.QColumnName _ _ toName) = fromName == toName
         in if sameCol from to
            then []
            else [ DropColumn $ void $ from { columnNameTable = Identity table }
                 , AddColumn $ void $ to { columnNameTable = Identity table }
                 ]
    getSchemaChange (AST.AlterTableAddColumns _ (AST.RTableName table _) (c:|cs)) =
        let toAddColumn uqcn = AddColumn $ void $ uqcn { columnNameTable = Identity table }
         in map toAddColumn (c:cs)

toUQCN :: AST.RColumnRef a -> UQColumnName ()
toUQCN (AST.RColumnRef (QColumnName _ _ column)) = QColumnName () None column
toUQCN (AST.RColumnAlias (ColumnAlias _ column _)) = QColumnName () None column

