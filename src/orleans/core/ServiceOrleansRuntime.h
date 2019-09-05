#pragma once

#include <memory>
#include <map>
#include <mutex>

#include <brynet/utils/NonCopyable.h>
#include <brynet/net/TCPService.h>
#include <brynet/net/ListenThread.h>
#include <gayrpc/core/GayRpcTypeHandler.h>
#include <gayrpc/core/GayRpcType.h>
#include <gayrpc/utils/UtilsWrapper.h>

#include <orleans/core/CoreType.h>
#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/orleans_service.pb.h>
#include <orleans/core/Utils.h>

namespace orleans { namespace core {

    using namespace brynet::net;

    class ServiceOrleansRuntime : public brynet::utils::NonCopyable, public std::enable_shared_from_this<ServiceOrleansRuntime>
    {
    public:
        using Ptr = std::shared_ptr<ServiceOrleansRuntime>;
        using GrainCreator = std::function<gayrpc::core::RpcTypeHandleManager::PTR(std::string)>;

    public:
        virtual ~ServiceOrleansRuntime() = default;

        ServiceOrleansRuntime(ServiceMetaManager::Ptr metaManager, 
            brynet::net::EventLoop::Ptr timerEventLoop)
            :
            mServiceMetaManager(metaManager),
            mTimerEventLoop(timerEventLoop)
        {
        }

        gayrpc::core::RpcTypeHandleManager::PTR findOrCreateServiceGrain(
            const std::string& grainType, 
            const std::string& grainUniqueName)
        {
            gayrpc::core::RpcTypeHandleManager::PTR grain;

            {
                std::lock_guard<std::mutex> lck(mGrainsGuard);

                if (const auto it = mServiceGrains.find(grainUniqueName); it != mServiceGrains.end())
                {
                    return it->second;
                }

                const auto it = mServceGrainCreators.find(grainType);
                if (it == mServceGrainCreators.end())
                {
                    return nullptr;
                }

                grain = it->second(grainUniqueName);
                if (grain)
                {
                    mServiceGrains[grainUniqueName] = grain;
                }
            }

            if (grain)
            {
                // 开启存活定时器
                onActiveTimer(grainUniqueName);
            }

            return grain;
        }

        // 释放grain
        void    releaseGrain(const std::string& grainUniqueName)
        {
            std::lock_guard<std::mutex> lck(mGrainsGuard);
            mServiceGrains.erase(grainUniqueName);
        }

        // 注册GrainType服务
        template<typename GrainType>
        auto registerServiceGrain(std::string addr, std::chrono::milliseconds timeout)
        {
            std::lock_guard<std::mutex> lck(mGrainsGuard);

            const auto typeName = GrainType::GetServiceTypeName();

            mServceGrainCreators[typeName] = [](std::string grainUniqueName) {
                // 创建Grain 服务
                auto grainRpcHandlerManager = std::make_shared<gayrpc::core::RpcTypeHandleManager>();
                gayrpc::core::ServiceContext serviceContext(grainRpcHandlerManager,
                    [=](gayrpc::core::RpcMeta&& meta,
                        const google::protobuf::Message& message,
                        gayrpc::core::UnaryHandler&& next,
                        gayrpc::core::InterceptorContextType&& context)
                    {
                        return next(std::move(meta), message, std::move(context));
                    },
                    [=](gayrpc::core::RpcMeta&& meta,
                        const google::protobuf::Message& message,
                        gayrpc::core::UnaryHandler&& next,
                        gayrpc::core::InterceptorContextType&& context)
                    {
                        // 处理业务层RPC服务的输出(即Response)

                        auto replyObj = context[OrleansReplyObjKey];
                        auto replyObjPtr = std::any_cast<orleans::core::OrleansServiceService::RequestReply::PTR>(replyObj);
                        assert(replyObjPtr != nullptr);
                        // 用底层RPC包装业务层的RPC Response
                        orleans::core::OrleansResponse response;
                        *(response.mutable_meta()) = meta;
                        response.set_body(message.SerializeAsString());
                        replyObjPtr->reply(response, std::move(context));

                        return next(std::move(meta), message, std::move(context));
                    });
                auto service = std::make_shared<GrainType>(std::move(serviceContext), grainUniqueName);
                GrainType::Install(service);

                return grainRpcHandlerManager;
            };

            return mServiceMetaManager->registerGrain(typeName, addr, timeout);
        }

        // 强制将某类型grain激活到某地址
        template<typename GrainType>
        auto createGrainByAddr(std::string grainID, std::string addr, std::chrono::milliseconds timeout)
        {
            const auto grainTypeName = GrainType::GetServiceTypeName();
            const auto grainUniqueName = Utils::MakeGrainUniqueName(grainTypeName, grainID);
            return mServiceMetaManager->createGrainByAddr(grainUniqueName, addr, timeout);
        }

    private:
        void    onActiveTimer(const std::string& grainUniqueName)
        {
            {
                // 如果此grain在本地已经不存在则退出函数
                std::lock_guard<std::mutex> lck(mGrainsGuard);
                auto it = mServiceGrains.find(grainUniqueName);
                if (it == mServiceGrains.end())
                {
                    return;
                }
            }

            mServiceMetaManager->activeGrain(grainUniqueName, std::chrono::seconds(10), std::chrono::seconds(10));

            auto sharedThis = shared_from_this();
            mTimerEventLoop->runAfter(std::chrono::seconds(10), [sharedThis, grainUniqueName]() {
                sharedThis->onActiveTimer(grainUniqueName);
            });
        }

    private:
        const ServiceMetaManager::Ptr                                   mServiceMetaManager;
        const brynet::net::EventLoop::Ptr                               mTimerEventLoop;
        std::vector<brynet::net::ListenThread::Ptr>                     mListenThreads;
        std::mutex                                                      mListenThreadsGuard;

        std::mutex                                                      mGrainsGuard;
        std::map<std::string, gayrpc::core::RpcTypeHandleManager::PTR>  mServiceGrains;
        std::map <GrainTypeName, GrainCreator>                          mServceGrainCreators;
    };
    
} }