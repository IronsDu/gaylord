#include "./pb/echo_service.gayrpc.h"

#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/ServiceOrleansRuntime.h>
#include <orleans/impl/OrleansGrainServiceImpl.h>
#include <orleans/impl/RedisServiceMetaManager.h>
#include <orleans/core/ClientOrleansRuntime.h>
#include <orleans/core/orleans_service.gayrpc.h>

const std::string hello("hello");
const std::string world("world");

using namespace gayrpc::core;
using namespace orleans::core;
using namespace orleans::impl;

const std::string ServiceIP("127.0.0.1");
const int ServicePort = 9999;

// 业务Grain服务
class MyEchoService : public dodo::test::EchoServerService
{
public:
    MyEchoService(gayrpc::core::ServiceContext&& context, std::string name)
        :
        dodo::test::EchoServerService(std::move(context))
    {
    }
    virtual void Echo(const dodo::test::EchoRequest& request,
        const dodo::test::EchoServerService::EchoReply::PTR& replyObj,
        InterceptorContextType&& context) override
    {
        assert(request.message() == hello);
        dodo::test::EchoResponse response;
        response.set_message(world);
        replyObj->reply(response, std::move(context));
    }
    virtual void Login(const dodo::test::LoginRequest& request,
        const dodo::test::EchoServerService::LoginReply::PTR& replyObj,
        InterceptorContextType&& context) override
    {
    }
};

int main()
{
    brynet::net::base::InitSocket();

    auto mainLoop = std::make_shared<brynet::net::EventLoop>();
    orleans::core::ServiceMetaManager::Ptr serviceMetaManager;
    {
        auto redisServiceMetaManager = std::make_shared<RedisServiceMetaManager>(mainLoop);
        redisServiceMetaManager->init("127.0.0.1", 6379);
        serviceMetaManager = redisServiceMetaManager;
    }

    auto service = brynet::net::TcpService::Create();
    service->startWorkerThread(1);

    auto serviceOrleansRuntime = std::make_shared<ServiceOrleansRuntime>(serviceMetaManager, mainLoop);

    // 开启节点通信服务
    auto serviceBulder = gayrpc::utils::ServiceBuilder<orleans::impl::OrleansGrainServiceImpl>();
    serviceBulder.configureCreator([=](ServiceContext&& context) {
            return std::make_shared<orleans::impl::OrleansGrainServiceImpl>(std::move(context), serviceOrleansRuntime);
        })
        .configureService(service)
        .configureConnectionOptions({ TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024) })
        .configureListen([=](wrapper::BuildListenConfig config) {
            config.setAddr(false, ServiceIP, ServicePort);
        })
        .asyncRun();

    // 注册Grain服务MyEchoService
    auto addr = Utils::MakeIpAddrString(ServiceIP, ServicePort);
    serviceOrleansRuntime->registerServiceGrain<MyEchoService>(addr);
    serviceOrleansRuntime->createGrainByAddr<MyEchoService>("123", addr);

    std::thread([serviceMetaManager]() {
        // 演示定期刷新本地缓存
        while(true) 
        {
            serviceMetaManager->updateGrairAddrList();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }).detach();
    std::cin.get();
    return 0;
}