#include "./pb/echo_service.gayrpc.h"

#include <orleans/impl/RedisServiceMetaManager.h>
#include <orleans/core/ClientOrleansRuntime.h>
#include <orleans/core/orleans_service.gayrpc.h>

const std::string hello("hello");
const std::string world("world");

using namespace gayrpc::core;
using namespace orleans::core;
using namespace orleans::impl;

int main()
{
    brynet::net::base::InitSocket();

    auto redisServiceMetaManager = std::make_shared<RedisServiceMetaManager>();
    redisServiceMetaManager->init("127.0.0.1", 6379);

    auto clientOrleansRuntime = std::make_shared<ClientOrleansRuntime>(redisServiceMetaManager);
    // 获取Grain
    auto echoServer1Grain = clientOrleansRuntime->takeGrain<dodo::test::EchoServerClient>("echo_server_1");

    for(auto i = 0; i < 10; i++)
    {
        dodo::test::EchoRequest request;
        request.set_message(hello);
        gayrpc::core::RpcError error;
        auto response = echoServer1Grain->SyncEcho(request, error, std::chrono::seconds(10));

        assert(response.message() == world);
    }

    return 0;
}