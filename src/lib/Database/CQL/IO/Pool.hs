-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

module Database.CQL.IO.Pool
    ( Pool
    , create
    , destroy
    , purge
    , with

    , PoolSettings
    , defSettings
    , idleTimeout
    , maxConnections
    , maxTimeouts
    , poolStripes
    ) where

import Control.AutoUpdate
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Lens ((^.), makeLenses, view)
import Control.Monad.IO.Class
import Control.Monad
import Data.Foldable (forM_, mapM_, find)
import Data.Function (on)
import Data.Hashable
import Data.IORef
import Data.Sequence (Seq, ViewL (..), (|>), (><))
import Data.Semigroup ((<>))
import Data.Time.Clock (UTCTime, NominalDiffTime, getCurrentTime, diffUTCTime)
import Data.Vector (Vector, (!))
import Database.CQL.IO.Connection (Connection)
import Database.CQL.IO.Exception (ConnectionError (..), ignore)
import Database.CQL.IO.Log

import qualified Data.Sequence as Seq
import qualified Data.Vector   as Vec

-----------------------------------------------------------------------------
-- API

data PoolSettings = PoolSettings
    { _idleTimeout    :: !NominalDiffTime
    , _maxConnections :: !Int
    , _maxTimeouts    :: !Int
    , _poolStripes    :: !Int
    }

type Pool = Pool_ Connection

data Pool_ conn = Pool
    { _createFn    :: !(IO conn)
    , _destroyFn   :: !(conn -> IO ())
    , _logger      :: !Logger
    , _settings    :: !PoolSettings
    , _maxRefs     :: !Int
    , _currentTime :: !(IO UTCTime)
    , _stripes     :: !(Vector (Stripe_ conn))
    , _finaliser   :: !(IORef ())
    }

type Resource = Resource_ Connection

data Resource_ conn = Resource
    { tstamp   :: !UTCTime
    , refcnt   :: !Int
    , timeouts :: !Int
    , value    :: !conn
    } deriving Show

type Box = Box_ Connection

data Box_ conn
    = New  !(IO (Resource_ conn))
    | Used !(Resource_ conn)
    | Empty

type Stripe = Stripe_ Connection

data Stripe_ conn = Stripe
    { conns :: !(TVar (Seq (Resource_ conn)))
    , inUse :: !(TVar Int)
    }

makeLenses ''PoolSettings
makeLenses ''Pool_

defSettings :: PoolSettings
defSettings = PoolSettings
    60 -- idle timeout
    2  -- max connections per stripe
    16 -- max timeouts per connection
    4  -- max stripes

create :: (Show conn) => IO conn -> (conn -> IO ()) -> Logger -> PoolSettings -> Int -> IO (Pool_ conn)
create mk del g s k = do
    p <- Pool mk del g s k
            <$> mkAutoUpdate defaultUpdateSettings { updateAction = getCurrentTime }
            <*> Vec.replicateM (s^.poolStripes) (Stripe <$> newTVarIO Seq.empty <*> newTVarIO 0)
            <*> newIORef ()
    r <- async $ reaper p
    void $ mkWeakIORef (p^.finaliser) (cancel r >> destroy p)
    return p

destroy :: Pool_ conn -> IO ()
destroy = purge

with :: (MonadIO m, Eq conn, Show conn) => Pool_ conn -> (conn -> IO a) -> m (Maybe a)
with p f = liftIO $ do
    s <- stripe p
    mask $ \restore -> do
        r <- take1 p s
        case r of
            Just  v -> do
                x <- restore (f (value v)) `catch` cleanup p s v
                put p s v id
                return (Just x)
            Nothing -> return Nothing

purge :: Pool_ conn -> IO ()
purge p = Vec.forM_ (p^.stripes) $ \s -> do
    cs <- atomically (swapTVar (conns s) Seq.empty)
    mapM_ (ignore . view destroyFn p . value) cs

-----------------------------------------------------------------------------
-- Internal

cleanup :: (Show conn, Eq conn) => Pool_ conn -> Stripe_ conn -> Resource_ conn -> SomeException -> IO a
cleanup p s r x = do
    case fromException x of
        Just (ResponseTimeout {}) -> onTimeout
        _                         -> destroyR p s r
    throwIO x
  where
    onTimeout =
        if timeouts r > p^.settings.maxTimeouts
            then do
                logInfo (p^.logger) $ string8 (show (value r)) <> ": Too many timeouts."
                destroyR p s r
            else put p s r incrTimeouts

take1 :: Pool_ conn -> Stripe_ conn -> IO (Maybe (Resource_ conn))
take1 p s = do
    r <- atomically $ do
        c <- readTVar (conns s)
        u <- readTVar (inUse s)
        let n = Seq.length c
        check (u == n)
        let r :< rr = Seq.viewl $ Seq.unstableSortBy (compare `on` refcnt) c
        if | u < p^.settings.maxConnections -> do
                writeTVar (inUse s) $! u + 1
                mkNew p
           | n > 0 && refcnt r < p^.maxRefs -> use s r rr
           | otherwise                      -> return Empty
    -- ASYNC KILL
    case r of
        New io -> do
            x <- io `onException` atomically (modifyTVar' (inUse s) (subtract 1))
            atomically (modifyTVar' (conns s) (|> x))
            return (Just x)
        Used x -> return (Just x)
        Empty  -> return Nothing

use :: Stripe_ conn -> Resource_ conn -> Seq (Resource_ conn) -> STM (Box_ conn)
use s r rr = do
    writeTVar (conns s) $! rr |> r { refcnt = refcnt r + 1 }
    return (Used r)
{-# INLINE use #-}

mkNew :: Pool_ conn -> STM (Box_ conn)
mkNew p = return (New $ Resource <$> p^.currentTime <*> pure 1 <*> pure 0 <*> p^.createFn)
{-# INLINE mkNew #-}

put :: (Eq conn) => Pool_ conn -> Stripe_ conn -> Resource_ conn -> (Resource_ conn -> Resource_ conn) -> IO ()
put p s r f = do
    now <- p^.currentTime
    let updated x = f x { tstamp = now, refcnt = refcnt x - 1 }
    atomically $ do
        rs <- readTVar (conns s)
        let (xs, rr) = Seq.breakl ((value r ==) . value) rs
        case Seq.viewl rr of
            EmptyL  -> writeTVar (conns s) $! xs         |> updated r
            y :< ys -> writeTVar (conns s) $! (xs >< ys) |> updated y

destroyR :: (Eq conn) => Pool_ conn -> Stripe_ conn -> Resource_ conn -> IO ()
destroyR p s r = do
    atomically $ do
        rs <- readTVar (conns s)
        case find ((value r ==) . value) rs of
            Nothing -> return ()
            Just  _ -> do
                modifyTVar' (inUse s) (subtract 1)
                writeTVar (conns s) $! Seq.filter ((value r /=) . value) rs
    ignore $ p^.destroyFn $ value r

reaper :: (Show conn) => Pool_ conn -> IO ()
reaper p = forever $ do
    threadDelay 1000000
    now <- p^.currentTime
    let isStale r = refcnt r == 0 && now `diffUTCTime` tstamp r > p^.settings.idleTimeout
    Vec.forM_ (p^.stripes) $ \s -> do
        x <- atomically $ do
                (stale, okay) <- Seq.partition isStale <$> readTVar (conns s)
                unless (Seq.null stale) $ do
                    writeTVar   (conns s) okay
                    modifyTVar' (inUse s) (subtract (Seq.length stale))
                return stale
        forM_ x $ \v -> ignore $ do
            logDebug (p^.logger) $ "Reaping idle connection: " <> string8 (show (value v))
            p^.destroyFn $ (value v)

stripe :: Pool_ conn -> IO (Stripe_ conn)
stripe p = ((p^.stripes) !) <$> ((`mod` (p^.settings.poolStripes)) . hash) <$> myThreadId
{-# INLINE stripe #-}

incrTimeouts :: Resource_ conn -> Resource_ conn
incrTimeouts r = r { timeouts = timeouts r + 1 }
{-# INLINE incrTimeouts #-}
