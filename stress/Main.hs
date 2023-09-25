{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}

import Database.CQL.IO.Pool
import Control.Concurrent.Async (async, Async, cancel, waitCatch)
import Database.CQL.IO.Log (nullLogger)
import System.Random (randomRIO)
import Data.Traversable (for)
import Data.Foldable (traverse_)
import Data.Unique
import Unsafe.Coerce (unsafeCoerce)
import Control.Monad (void, foldM)
import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.MVar (newMVar, modifyMVar_, MVar, readMVar)
import System.Random.Shuffle (shuffleM)
import System.Environment (getArgs)
import Control.Exception (throwTo, AsyncException (..))
import System.IO (hFlush, stdout)

newtype Conn = Conn Unique
  deriving newtype (Eq)

instance Show Conn where
  show (Conn u) = show @Integer $ unsafeCoerce u

newConn :: IO Conn
newConn = Conn <$> newUnique

main :: IO ()
main = do
  count <- getArg
  putStrLn $ "Trying " <> show count <> " threads"
  pool <- create newConn nothing nullLogger defSettings 5
  counter <- newMVar 0
  threads :: [Async ()] <- for [1..count] $ \_ ->
    async (retry $ with pool $ const (task counter))
  shuffled <- shuffleM threads
  toKill <- randomRIO (0, count)
  traverse_ cancel $ take toKill shuffled
  forkIO $ tick counter
  traverse_ waitCatch threads
  done <- readMVar counter
  putStrLn $ "Done " <> show done <> " killed " <> show toKill
  where
    nothing = const $ pure ()
    getArg :: IO Int
    getArg = read . head <$> getArgs
    tick counter = do
      count <- readMVar counter
      putStrLn $ "tick " <> show count
      hFlush stdout
      threadDelay 1_000_000
      tick counter

task :: MVar Int -> IO ()
task counter = do
  n <- randomRIO @Int (0, 10)
  threadDelay (n * 1_000)
  modifyMVar_ counter (pure . (+ 1))
  putStr "." *> hFlush stdout


retry :: IO (Maybe a) -> IO a
retry io = do
  io >>= \case
    Just a -> pure a
    Nothing -> threadDelay 1_000 *> retry io
