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
    MyEchoService(gayrpc::core::ServiceContext context)
        :
        dodo::test::EchoServerService(context)
    {}
    virtual void Echo(const dodo::test::EchoRequest& request,
        const dodo::test::EchoServerService::EchoReply::PTR& replyObj,
        InterceptorContextType context)
    {
        assert(request.message() == hello);
        dodo::test::EchoResponse response;
        response.set_message(world);
        replyObj->reply(response, std::move(context));
    }
    virtual void Login(const dodo::test::LoginRequest& request,
        const dodo::test::EchoServerService::LoginReply::PTR& replyObj,
        InterceptorContextType context)
    {
    }
};

int main()
{
    brynet::net::base::InitSocket();

    auto mainLoop = std::make_shared<brynet::net::EventLoop>();
    auto redisServiceMetaManager = std::make_shared<RedisServiceMetaManager>(mainLoop);
    redisServiceMetaManager->init("127.0.0.1", 6379);

    auto serviceOrleansRuntime = std::make_shared<ServiceOrleansRuntime>(redisServiceMetaManager);
    // 开启节点通信服务
    serviceOrleansRuntime->startTCPService<OrleansGrainServiceImpl>(ServiceIP, ServicePort);
    // 注册Grain服务MyEchoService
    auto addr = ServiceIP + ":" + std::to_string(ServicePort);
    serviceOrleansRuntime->registerServiceGrain<MyEchoService>(addr);
    
    std::cin.get();
    return 0;
}