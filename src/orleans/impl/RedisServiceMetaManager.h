#pragma once

#include <cpp_redis/core/client.hpp>
#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/CoreType.h>
#include <brynet/net/EventLoop.h>

namespace orleans { namespace impl {

    using namespace orleans::core;

    class RedisServiceMetaManager : public ServiceMetaManager, public std::enable_shared_from_this<RedisServiceMetaManager>
    {
    public:
        RedisServiceMetaManager(brynet::net::EventLoop::Ptr timerEventLoop)
            :
            mRedisClient(std::make_shared<cpp_redis::client>()),
            mTimerEventLoop(timerEventLoop)
        {
        }

        void    init(const std::string& redisIP, int port)
        {
            mRedisClient->connect(redisIP, port, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {
                if (status == cpp_redis::client::connect_state::dropped) {
                    std::cout << "client disconnected from " << host << ":" << port << std::endl;
                }
            });
        }

    private:
        void    registerGrain(GrainTypeName grainTypeName, std::string addr) override
        {
            mRedisClient->lpush(grainTypeName, { addr });
            mRedisClient->sync_commit();
        }

        void    queryGrainAddr(GrainTypeName grainTypeName, std::string grainUniqueName, ServiceMetaManager::QueryGrainCompleted caller) override
        {
            {
                std::lock_guard<std::mutex> lck(mGrainAddrCacheGuard);
                {
                    const auto it = mGrainAddrCache.find(grainUniqueName);
                    if (it != mGrainAddrCache.end())
                    {
                        caller((*it).second);
                        return;
                    }
                }
                {
                    const auto it = mGrainAddrActiveCache.find(grainUniqueName);
                    if (it != mGrainAddrActiveCache.end())
                    {
                        caller((*it).second);
                        return;
                    }
                }
            }
            // 从Redis里查找路由信息
            mRedisClient->get(grainUniqueName, [=](cpp_redis::reply& reply) {
                if (!reply.is_null())
                {
                    const auto addr = reply.as_string();
                    addGrainAddrToCache(grainUniqueName, addr);
                    caller(addr);
                }
                else
                {
                    // 若不存在则获取处理此类型服务的所有服务器
                    mRedisClient->lrange(grainTypeName, 0, -1, [=](cpp_redis::reply& reply) {
                        if (!reply.is_array())
                        {
                            return;
                        }
                        auto addrs = reply.as_array();
                        if(addrs.empty())
                        {
                            return;
                        }

                        //随机一个节点服务器
                        auto addr = addrs[std::rand() % addrs.size()].as_string();
                        mRedisClient->setnx(grainUniqueName, addr, [=](cpp_redis::reply& reply) {
                            if (!reply.ok() || !reply.is_integer())
                            {
                                return;
                            }
                            if (reply.as_integer() == 1)
                            {
                                addGrainAddrToCache(grainUniqueName, addr);
                                caller(addr);
                                return;
                            }
                            else if (reply.as_integer() == 0)
                            {
                                mRedisClient->get(grainUniqueName, [=](cpp_redis::reply& reply) {
                                    if (reply.is_null())
                                    {
                                        return;
                                    }

                                    const auto addr = reply.as_string();
                                    addGrainAddrToCache(grainUniqueName, addr);
                                    caller(addr);
                                });
                                mRedisClient->commit();
                            }
                        });
                        mRedisClient->commit();
                    });
                    mRedisClient->commit();
                }
            });
            mRedisClient->commit();
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

        void    startActiveTimer(std::string grainUniqueName) override
        {
            mRedisClient->expire(grainUniqueName, 20);
            mRedisClient->commit();

            mTimerEventLoop->pushAsyncFunctor([grainUniqueName, timerLoop = mTimerEventLoop, sharedThis = shared_from_this()]() {
                timerLoop->getTimerMgr()->addTimer(std::chrono::seconds(10), [=]() {
                    sharedThis->startActiveTimer(grainUniqueName);
                });
            });
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
        const std::shared_ptr<cpp_redis::client>    mRedisClient;
        const brynet::net::EventLoop::Ptr           mTimerEventLoop;
        std::map<std::string, std::string>          mGrainAddrCache;        // Grain缓存
        std::map<std::string, std::string>          mGrainAddrActiveCache;  // 当前确认活跃Grain
        std::mutex                                  mGrainAddrCacheGuard;
    };

} }