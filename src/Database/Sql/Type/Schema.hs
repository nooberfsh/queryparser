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
import Control.Monad.Except
import Control.Monad.Writer
import Data.Functor.Identity

import qualified Data.HashMap.Strict as HMS

import Data.Maybe (mapMaybe, maybeToList)

and3 :: Bool -> Bool -> Bool -> Bool
and3 x y z = x && y && z

overWithColumns :: (r a -> s a) -> WithColumns r a -> WithColumns s a
overWithColumns f (WithColumns r cs) = WithColumns (f r) cs

