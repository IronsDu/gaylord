#pragma once

#include <optional>
#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/CoreType.h>
#include <brynet/net/EventLoop.h>
#include <brynet_redis/RedisClient.h>

namespace orleans { namespace impl {

    using namespace orleans::core;

    class RedisServiceMetaManager : public ServiceMetaManager, public std::enable_shared_from_this<RedisServiceMetaManager>
    {
    public:
        RedisServiceMetaManager(brynet::net::EventLoop::Ptr timerEventLoop,
            brynet::net::TcpService::Ptr tcpService,
            brynet::net::AsyncConnector::Ptr connector)
            :
            mRedisClient(std::make_shared<SuperRedisClient>(tcpService, connector))
        {
        }

        auto    init(const std::string& redisIP, int port, std::chrono::milliseconds timeout)
        {
            RedisConnectConfig config;
            config.ip = redisIP;
            config.port = port;
            config.connectTimeout = timeout;
            return mRedisClient->asyncConnect(config);
        }

    private:
        ananas::Future<bool> registerGrain(GrainTypeName grainTypeName, 
            std::string addr, 
            std::chrono::milliseconds timeout) override
        {
            ananas::Promise<bool> promise;
            mRedisClient
                ->lpush(grainTypeName, { addr }, timeout)
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid())
                    {
                        promise.SetValue(false);
                        return;
                    }
                    promise.SetValue(reply.isOk());
                });
            return promise.GetFuture();
        }

        // 查询Grain地址
        ananas::Future<std::string>    queryGrainAddr(GrainTypeName grainTypeName,
            std::string grainUniqueName,
            std::chrono::milliseconds timeout) override
        {
            {
                std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
                {
                    const auto it = mGrainAddrCache.find(grainUniqueName);
                    if (it != mGrainAddrCache.end())
                    {
                        return ananas::MakeReadyFuture((*it).second);
                    }
                }
                {
                    const auto it = mGrainAddrActiveCache.find(grainUniqueName);
                    if (it != mGrainAddrActiveCache.end())
                    {
                        return ananas::MakeReadyFuture((*it).second);
                    }
                }
            }

            ananas::Promise<std::string> promise;
            auto sharedThis = shared_from_this();
            auto redisClient = mRedisClient;

            mRedisClient
                ->get(grainUniqueName, timeout)
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid() || reply.isNull())
                    {
                        promise.SetValue(std::string(""));
                        return;
                    }

                    const std::string addr = reply;
                    sharedThis->addGrainAddrToCache(grainUniqueName, addr);
                    promise.SetValue(addr);
                });
            return promise.GetFuture();
        }

        // 查询或分配Grain地址
        ananas::Future<std::string>    queryOrCreateGrainAddr(GrainTypeName grainTypeName,
            std::string grainUniqueName,
            std::chrono::milliseconds timeout) override
        {
            {
                std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
                {
                    const auto it = mGrainAddrCache.find(grainUniqueName);
                    if (it != mGrainAddrCache.end())
                    {
                        return ananas::MakeReadyFuture((*it).second);
                    }
                }
                {
                    const auto it = mGrainAddrActiveCache.find(grainUniqueName);
                    if (it != mGrainAddrActiveCache.end())
                    {
                        return ananas::MakeReadyFuture((*it).second);
                    }
                }
            }

            ananas::Promise<std::string> promise;
            auto sharedThis = shared_from_this();
            auto redisClient = mRedisClient;

            auto addr = std::make_shared<std::string>();

            mRedisClient
                ->get(grainUniqueName, timeout)
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid())
                    {
                        promise.SetValue(std::string(""));
                        return ananas::Future<RedisReply>();
                    }
                    if (!reply.isNull())
                    {
                        const std::string addr = reply;
                        sharedThis->addGrainAddrToCache(grainUniqueName, addr);
                        promise.SetValue(addr);

                        return ananas::Future<RedisReply>();
                    }

                    return redisClient->lrange(grainTypeName, 0, -1, timeout);
                })
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid() || !reply.isArray())
                    {
                        promise.SetValue(std::string(""));
                        return ananas::Future<RedisReply>();
                    }

                    const std::vector<RedisReply> addrs = reply;
                    if (addrs.empty())
                    {
                        promise.SetValue(std::string(""));
                        return ananas::Future<RedisReply>();
                    }

                    *addr = (std::string)(addrs[std::rand() % addrs.size()]);
                    return redisClient->setnx(grainUniqueName, *addr, timeout);
                })
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid() || !reply.isOk() || !reply.isInteger())
                    {
                        promise.SetValue(std::string(""));
                        return ananas::Future<RedisReply>();
                    }
                    const int64_t num = reply;
                    if (num == 1)
                    {
                        sharedThis->addGrainAddrToCache(grainUniqueName, *addr);
                        promise.SetValue(*addr);
                        return ananas::Future<RedisReply>();
                    }
                    else if (num == 0)
                    {
                        return redisClient->get(grainUniqueName, timeout);
                    }
                    else
                    {
                        return ananas::Future<RedisReply>();
                    }
                })
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid() || reply.isNull() || !reply.isOk())
                    {
                        promise.SetValue(std::string(""));
                        return;
                    }

                    const std::string addr = reply;
                    sharedThis->addGrainAddrToCache(grainUniqueName, addr);
                    promise.SetValue(addr);
                });
            return promise.GetFuture();
        }

        void    processAddrStatus(std::string grainUniqueName, std::string addr, bool isGood) override
        {
            std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
            if (isGood)
            {
                const auto it = mGrainAddrActiveCache.find(grainUniqueName);
                if (it != mGrainAddrActiveCache.end())
                {
                    return;
                }
                mGrainAddrActiveCache[grainUniqueName] = addr;
            }
            else
            {
                mGrainAddrActiveCache.erase(grainUniqueName);
            }
        }

        // 存活某Grain
        virtual ananas::Future<bool>    activeGrain(std::string grainUniqueName,
            std::chrono::seconds expire,
            std::chrono::milliseconds timeout) override
        {
            ananas::Promise<bool> promise;
            mRedisClient->expire(grainUniqueName, expire.count(), timeout)
                .Then([=](RedisReply r) mutable {
                    if (!r.isValid())
                    {
                        promise.SetValue(false);
                        return;
                    }
                    promise.SetValue(r.isOk());
                });
            return promise.GetFuture();
        }

        virtual ananas::Future<bool>    createGrainByAddr(std::string grainUniqueName, 
            std::string addr, 
            std::chrono::milliseconds timeout) override
        {
            {
                std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
                if (mGrainAddrCache.find(grainUniqueName) != mGrainAddrCache.end())
                {
                    return ananas::MakeReadyFuture<bool>(true);
                }
                if (mGrainAddrActiveCache.find(grainUniqueName) != mGrainAddrActiveCache.end())
                {
                    return ananas::MakeReadyFuture<bool>(true);
                }
            }

            ananas::Promise<bool> promise;
            mRedisClient->setnx(grainUniqueName, addr, timeout)
                .Then([=](RedisReply reply) mutable {
                    if (!reply.isValid())
                    {
                        promise.SetValue(false);
                        return;
                    }
                    promise.SetValue(reply.isOk());
                });
            return promise.GetFuture();
        }

        void    updateGrairAddrList()
        {
            std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
            mGrainAddrCache = mGrainAddrActiveCache;
            mGrainAddrActiveCache.clear();
        }

        void    addGrainAddrToCache(std::string grainUniqueName, std::string addr)
        {
            std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
            mGrainAddrCache[grainUniqueName] = addr;
        }

    private:
        const SuperRedisClient::Ptr                 mRedisClient;
        std::map<std::string, std::string>          mGrainAddrCache;        // Grain缓存
        std::map<std::string, std::string>          mGrainAddrActiveCache;  // 最近确认活跃的Grain
        std::mutex                                  mGrainAddrCacheGuard;
    };

} }