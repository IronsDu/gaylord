#include <array>

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

    auto service = brynet::net::TcpService::Create();
    service->startWorkerThread(1);
    auto connector = brynet::net::AsyncConnector::Create();
    connector->startWorkerThread();

    auto mainLoop = std::make_shared<brynet::net::EventLoop>();
    auto redisServiceMetaManager = std::make_shared<RedisServiceMetaManager>(mainLoop, service, connector);

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
        std::vector< UnaryServerInterceptor>{},
        std::vector< UnaryServerInterceptor>{});

    redisServiceMetaManager->init("127.0.0.1", 6379, std::chrono::seconds(10))
        .Then([=](bool result) {
            if (!result)
            {
                return;
            }

            mainLoop->runAsyncFunctor([=]() {
                // 获取Grain
                auto echoServer1Grain = clientOrleansRuntime->takeGrain<dodo::test::EchoServerClient>("1");

                dodo::test::EchoRequest request;
                request.set_message(hello);
                echoServer1Grain
                    ->SyncEcho(request, std::chrono::seconds(1))
                    .Then([echoServer1Grain](std::pair<dodo::test::EchoResponse, gayrpc::core::RpcError> result) {

                        std::cout << result.first.message() << std::endl;
                        dodo::test::EchoRequest request;
                        request.set_message(hello);
                        return echoServer1Grain
                            ->SyncEcho(request, std::chrono::seconds(1));
                    })
                    .Then([](std::pair<dodo::test::EchoResponse, gayrpc::core::RpcError> result) {
                        std::cout << result.first.message() << std::endl;
                    });

                for (auto i = 0; i < 10; i++)
                {
                    dodo::test::EchoRequest request;
                    request.set_message(hello);
                    auto responseFuture = echoServer1Grain->SyncEcho(request, std::chrono::seconds(10));
                    auto response = responseFuture.Wait();
                    std::cout << response.Value().first.message() << std::endl;
                    assert(response.Value().first.message() == world);
                }

                clientOrleansRuntime->releaseGrain<dodo::test::EchoServerClient>("1");
            });
        });

    for (;;)
    {
        mainLoop->loop(std::chrono::milliseconds(100).count());
    }
    std::cin.get();
    return 0;
}