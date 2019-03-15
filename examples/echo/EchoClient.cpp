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

    auto mainLoop = std::make_shared<brynet::net::EventLoop>();
    auto redisServiceMetaManager = std::make_shared<RedisServiceMetaManager>(mainLoop);
    redisServiceMetaManager->init("127.0.0.1", 6379);

    auto service = brynet::net::TcpService::Create();
    auto connector = brynet::net::AsyncConnector::Create();

    service->startWorkerThread(1);
    connector->startWorkerThread();

    auto clientOrleansRuntime = std::make_shared<ClientOrleansRuntime>(service,
        connector,
        redisServiceMetaManager,
        std::vector<AsyncConnector::ConnectOptions::ConnectOptionFunc>{
            AsyncConnector::ConnectOptions::WithTimeout(std::chrono::seconds(10)),
        },
        std::vector<TcpService::AddSocketOption::AddSocketOptionFunc>{
            TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024),
            TcpService::AddSocketOption::AddEnterCallback([](const TcpConnection::Ptr& session) {
                session->setHeartBeat(std::chrono::seconds(10));
            })
        },
        std::vector<RpcConfig::AddRpcConfigFunc>{});
    // 获取Grain
    auto echoServer1Grain = clientOrleansRuntime->takeGrain<dodo::test::EchoServerClient>("1");

    for(auto i = 0; i < 10; i++)
    {
        dodo::test::EchoRequest request;
        request.set_message(hello);
        gayrpc::core::RpcError error;
        auto response = echoServer1Grain->SyncEcho(request, error, std::chrono::seconds(10));

        assert(response.message() == world);
    }

    clientOrleansRuntime->releaseGrain<dodo::test::EchoServerClient>("1");

    return 0;
}