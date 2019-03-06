#pragma once

#include <memory>
#include <map>
#include <mutex>

#include <brynet/utils/NonCopyable.h>
#include <brynet/net/TCPService.h>
#include <brynet/net/ListenThread.h>
#include <gayrpc/core/GayRpcTypeHandler.h>
#include <gayrpc/core/GayRpcType.h>

#include <orleans/core/CoreType.h>
#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/orleans_service.pb.h>

namespace orleans { namespace core {

    class ServiceOrleansRuntime : public brynet::utils::NonCopyable, public std::enable_shared_from_this<ServiceOrleansRuntime>
    {
    public:
        using Ptr = std::shared_ptr<ServiceOrleansRuntime>;
        using GrainCreator = std::function<gayrpc::core::RpcTypeHandleManager::PTR(std::string)>;

    public:
        virtual ~ServiceOrleansRuntime() = default;

        explicit ServiceOrleansRuntime(ServiceMetaManager::Ptr metaManager, brynet::net::EventLoop::Ptr timerEventLoop)
            :
            mServiceMetaManager(metaManager),
            mTimerEventLoop(timerEventLoop),
            mTCPService(brynet::net::TcpService::Create()),
            mListenThread(brynet::net::ListenThread::Create())
        {
            mTCPService->startWorkerThread(1);
        }

        template<typename GrainServiceType>
        void    startTCPService(const std::string& ip, int port)
        {
            auto sharedThis = shared_from_this();
            gayrpc::utils::StartBinaryRpcServer<GrainServiceType>(mTCPService, mListenThread,
                ip, port,
                [=](gayrpc::core::ServiceContext context) {
                    return std::make_shared<GrainServiceType>(context, sharedThis);
                }, nullptr, nullptr, nullptr, 1024 * 1024, std::chrono::seconds(10));
        }

        gayrpc::core::RpcTypeHandleManager::PTR findOrCreateServiceGrain(const std::string& grainType, const std::string& grainUniqueName)
        {
            gayrpc::core::RpcTypeHandleManager::PTR grain;

            {
                std::lock_guard<std::mutex> lck(mGrainsGrard);

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
            std::lock_guard<std::mutex> lck(mGrainsGrard);
            mServiceGrains.erase(grainUniqueName);
        }

        // 注册GrainType服务
        template<typename GrainType>
        void registerServiceGrain(std::string addr)
        {
            std::lock_guard<std::mutex> lck(mGrainsGrard);

            auto typeName = GrainType::GetServiceTypeName();

            mServiceMetaManager->registerGrain(typeName, addr);
            mServceGrainCreators[typeName] = [](std::string grainUniqueName) {
                // 创建Grain 服务
                auto grainRpcHandlerManager = std::make_shared<gayrpc::core::RpcTypeHandleManager>();
                gayrpc::core::ServiceContext serviceContext(grainRpcHandlerManager,
                    [=](const gayrpc::core::RpcMeta& meta,
                        const google::protobuf::Message& message,
                        const gayrpc::core::UnaryHandler& next,
                        gayrpc::core::InterceptorContextType context)
                    {
                        return next(meta, message, std::move(context));
                    },
                    [=](const gayrpc::core::RpcMeta& meta,
                        const google::protobuf::Message& message,
                        const gayrpc::core::UnaryHandler& next,
                        gayrpc::core::InterceptorContextType context)
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

                        return next(meta, message, std::move(context));
                    });
                auto service = std::make_shared<GrainType>(serviceContext);
                GrainType::Install(service);

                return grainRpcHandlerManager;
            };
        }

    private:
        void    onActiveTimer(const std::string& grainUniqueName)
        {
            {
                // 如果此grain在本地已经不存在则退出函数
                std::lock_guard<std::mutex> lck(mGrainsGrard);
                auto it = mServiceGrains.find(grainUniqueName);
                if (it == mServiceGrains.end())
                {
                    return;
                }
            }

            mServiceMetaManager->activeGrain(grainUniqueName);

            auto sharedThis = shared_from_this();
            mTimerEventLoop->pushAsyncFunctor([sharedThis, grainUniqueName, timerEventLoop = mTimerEventLoop]() {
                timerEventLoop->getTimerMgr()->addTimer(std::chrono::seconds(10), [=]() {
                    sharedThis->onActiveTimer(grainUniqueName);
                });
                
            });
        }

    private:
        const ServiceMetaManager::Ptr                                   mServiceMetaManager;
        const brynet::net::EventLoop::Ptr                               mTimerEventLoop;
        const brynet::net::TcpService::Ptr                              mTCPService;
        const brynet::net::ListenThread::Ptr                            mListenThread;

        std::mutex                                                      mGrainsGrard;
        std::map<std::string, gayrpc::core::RpcTypeHandleManager::PTR>  mServiceGrains;
        std::map <GrainTypeName, GrainCreator>                          mServceGrainCreators;
    };
    
} }