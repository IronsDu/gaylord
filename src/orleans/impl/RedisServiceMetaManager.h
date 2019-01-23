#pragma once

#include <cpp_redis/core/client.hpp>
#include <orleans/core/ServiceMetaManager.h>

namespace orleans { namespace impl {

    class RedisServiceMetaManager : public ServiceMetaManager
    {
    public:
        RedisServiceMetaManager()
            :
            mRedisClient(std::make_shared<cpp_redis::client>())
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
        void    Register(GrainTypeName grainTypeName, std::string addr) override
        {
            mRedisClient->lpush(grainTypeName, { addr });
            mRedisClient->sync_commit();
        }

        void    QueryGrainAddr(GrainTypeName grainTypeName, std::string grainName, ServiceMetaManager::QueryGrainCompleted caller)
        {
            auto grainID = grainTypeName + ":" + grainName;
            // 从Redis里查找路由信息
            mRedisClient->get(grainID, [=](cpp_redis::reply& reply) {
                if (!reply.is_null())
                {
                    caller(reply.as_string());
                }
                else
                {
                    // 若不存在则获取处理此类型服务的所有服务器
                    mRedisClient->lrange(grainTypeName, 0, -1, [=](cpp_redis::reply& reply) {
                        if (reply.is_array())
                        {
                            auto addrs = reply.as_array();
                            //随机一个节点服务器
                            auto addr = addrs[std::rand() % addrs.size()].as_string();
                            mRedisClient->set(grainID, addr, [=](cpp_redis::reply& reply) {
                                if (reply.ok())
                                {
                                    caller(addr);
                                }
                                });
                            mRedisClient->commit();
                        }
                        });
                    mRedisClient->commit();
                }
                });
            mRedisClient->commit();
        }

    private:
        const std::shared_ptr<cpp_redis::client>    mRedisClient;
    };

} }