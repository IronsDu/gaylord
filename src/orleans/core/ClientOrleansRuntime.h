#pragma once

#include <memory>
#include <brynet/net/Connector.h>
#include <brynet/net/TCPService.h>
#include <brynet/utils/NonCopyable.h>
#include <gayrpc/utils/UtilsWrapper.h>
#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/orleans_service.gayrpc.h>
#include <orleans/core/Utils.h>

namespace orleans { namespace core {

    using namespace brynet::net;
    using namespace gayrpc::utils;

    class ClientOrleansRuntime : public brynet::utils::NonCopyable, public std::enable_shared_from_this<ClientOrleansRuntime>
    {
    public:
        using Ptr = std::shared_ptr< ClientOrleansRuntime>;
        using OrleansConnectionCreatedCallback = std::function<void(orleans::core::OrleansServiceClient::PTR)>;

    public:
        virtual ~ClientOrleansRuntime() = default;

        ClientOrleansRuntime(brynet::net::TcpService::Ptr service,
            brynet::net::AsyncConnector::Ptr connector,
            ServiceMetaManager::Ptr metaManager,
            std::vector<AsyncConnector::ConnectOptions::ConnectOptionFunc> connectOptions,
            std::vector<TcpService::AddSocketOption::AddSocketOptionFunc> socketOptions,
            std::vector< UnaryServerInterceptor> inboundInterceptors,
            std::vector< UnaryServerInterceptor> outboundInterceptors)
            :
            mServiceMetaManager(metaManager),
            mConnectOptions(std::move(connectOptions))
        {
            mRpcClientBuilder.configureConnector(connector)
                .configureService(service)
                .configureConnectionOptions(socketOptions)
                .buildInboundInterceptor([=](BuildInterceptor build) {
                    for (const auto& v : inboundInterceptors)
                    {
                        build.addInterceptor(v);
                    }
                })
                .buildOutboundInterceptor([=](BuildInterceptor build) {
                    for (const auto& v : inboundInterceptors)
                    {
                        build.addInterceptor(v);
                    }
                });
        }

        // 获取grain
        template<typename GrainType>
        auto takeGrain(std::string grainID)
        {
            const auto grainTypeName = GrainType::GetServiceTypeName();
            const auto grainUniqueName = Utils::MakeGrainUniqueName(grainTypeName, grainID);

            auto sharedThis = shared_from_this();
            auto grainRpcHandlerManager = std::make_shared<gayrpc::core::RpcTypeHandleManager>();

            auto grain = GrainType::Create(grainRpcHandlerManager,
                [=](gayrpc::core::RpcMeta&& meta,
                    const google::protobuf::Message& message,
                    gayrpc::core::UnaryHandler&& next,
                    InterceptorContextType&& context)
                {
                    return next(std::move(meta), message, std::move(context));
                },
                [=, serviceMetaManager = mServiceMetaManager](gayrpc::core::RpcMeta&& meta,
                    const google::protobuf::Message& message,
                    gayrpc::core::UnaryHandler&& next,
                    InterceptorContextType&& context)
                {
                    auto seq_id = meta.request_info().sequence_id();
                    auto timeoutSecond = meta.request_info().timeout();
                    // outboundInterceptor
                    // 处理业务层RPC Client的输出(即Request)
                    // 将业务RPC包裹在底层的OrleansRequest rpc中
                    orleans::core::OrleansRequest request;
                    request.set_grain_type(grainTypeName);
                    request.set_grain_unique_name(grainUniqueName);
                    *request.mutable_meta() = meta;
                    request.set_body(message.SerializeAsString());

                    std::shared_ptr<google::protobuf::Message> p(message.New());
                    p->CopyFrom(message);

                    // 尝试创建到grain所在节点的RPC
                    auto sharedAddr = std::make_shared<std::string>();
                    serviceMetaManager
                        ->queryOrCreateGrainAddr(grainTypeName, grainUniqueName, std::chrono::seconds(10))
                        .Then([=](std::string addr) {
                            if (addr.empty())
                            {
                                return ananas::Future<orleans::core::OrleansServiceClient::PTR>();
                            }
                            *sharedAddr = addr;
                            auto ipAddr = Utils::GetIPAddrFromString(addr);
                            return sharedThis->asyncOrleansConnectionCreated(ipAddr);
                        })
                        .Then([=](orleans::core::OrleansServiceClient::PTR orleanClient) {
                            if (orleanClient == nullptr)
                            {
                                return ananas::Future<std::pair<orleans::core::OrleansResponse, gayrpc::core::RpcError>>();
                            }
                            auto timeout = std::chrono::seconds(timeoutSecond > 0 ? timeoutSecond : 10);
                            // 向grain所在服务节点发送请求
                            return orleanClient->SyncRequest(request, timeout);
                        })
                        .Then([=, context = std::move(context), meta = std::move(meta)](
                            std::pair<orleans::core::OrleansResponse, gayrpc::core::RpcError> pairResonse) mutable {

                            const auto& response = pairResonse.first;
                            const auto& error = pairResonse.second;

                            if (error.timeout())
                            {
                                // 超时处理此grain地址状态
                                serviceMetaManager->processAddrStatus(grainUniqueName, *sharedAddr, false);
                                return;
                            }
                            
                            // TODO::remove if error.failed()
                            // 将收到的response交给用户层RPC
                            if (error.failed())
                            {
                                gayrpc::core::RpcMeta errorMeta;
                                errorMeta.set_type(gayrpc::core::RpcMeta::RESPONSE);
                                errorMeta.mutable_response_info()->set_sequence_id(seq_id);
                                errorMeta.mutable_response_info()->set_failed(true);
                                errorMeta.mutable_response_info()->set_error_code(error.code());
                                errorMeta.mutable_response_info()->set_reason(error.reason());
                                try
                                {
                                    InterceptorContextType context;
                                    grainRpcHandlerManager->handleRpcMsg(std::move(errorMeta), "", std::move(context));
                                }
                                catch (...)
                                {
                                }
                            }
                            else
                            {
                                InterceptorContextType context;
                                auto meta = response.meta();
                                grainRpcHandlerManager->handleRpcMsg(std::move(meta), 
                                    response.body(),
                                    std::move(context));
                            }
                            // 每次调用成功都处理此grain地址状态
                            serviceMetaManager->processAddrStatus(grainUniqueName, *sharedAddr, true);
                            next(std::move(meta), *p, std::move(context));
                        });
                });

            return grain;
        }

        // 释放grain
        template<typename GrainType>
        void    releaseGrain(std::string grainID)
        {
            releaseGrain(GrainType::GetServiceTypeName(), grainID);
        }

        // 释放grain
        void    releaseGrain(GrainTypeName grainTypeName, std::string grainID)
        {
            const auto grainUniqueName = Utils::MakeGrainUniqueName(grainTypeName, grainID);
            auto sharedThis = shared_from_this();
            
            mServiceMetaManager
                ->queryGrainAddr(grainTypeName, grainUniqueName, std::chrono::seconds(10))
                .Then([=](std::string addr) {
                    if (addr.empty())
                    {
                        return ananas::Future<orleans::core::OrleansServiceClient::PTR>();
                    }
                    auto ipAddr = Utils::GetIPAddrFromString(addr);
                    return sharedThis->asyncOrleansConnectionCreated(ipAddr);
                })
                .Then([=](orleans::core::OrleansServiceClient::PTR orleanClient) {
                    if (orleanClient == nullptr)
                    {
                        return ananas::Future<std::pair<orleans::core::OrleansReleaseResponse, gayrpc::core::RpcError>>();
                    }
                    orleans::core::OrleansReleaseRequest request;
                    request.set_grain_type(grainTypeName);
                    request.set_grain_unique_name(grainUniqueName);
                    return orleanClient->SyncRelease(request, std::chrono::seconds(10));
                })
                .Then([](std::pair<orleans::core::OrleansReleaseResponse, gayrpc::core::RpcError>) {
                });
        }

    private:
        orleans::core::OrleansServiceClient::PTR findOrleanConnection(IPAddr addr)
        {
            std::lock_guard<std::mutex> lck(mOrleansConnectionGrard);

            const auto it = mOrleans.find(addr);
            if (it == mOrleans.end())
            {
                return nullptr;
            }

            return it->second;
        }

        ananas::Future<orleans::core::OrleansServiceClient::PTR> 
            asyncOrleansConnectionCreated(IPAddr addr)
        {
            ananas::Promise<orleans::core::OrleansServiceClient::PTR> promise;

            auto orleans = findOrleanConnection(addr);
            if (orleans != nullptr)
            {
                // 直接执行回调
                return ananas::MakeReadyFuture(orleans);
            }
            else
            {
                // 如果当前没有到节点的链接则异步创建
                auto options = mConnectOptions;
                options.push_back(AsyncConnector::ConnectOptions::WithAddr(addr.first, addr.second));
                options.push_back(AsyncConnector::ConnectOptions::WithFailedCallback([=]() mutable {
                        promise.SetValue(orleans::core::OrleansServiceClient::PTR(nullptr));
                    }));
                mRpcClientBuilder
                    .configureConnectOptions(options)
                    .asyncConnect<orleans::core::OrleansServiceClient>(
                        [=](std::shared_ptr<orleans::core::OrleansServiceClient> client) mutable {
                            // RPC对象创建成功则执行回调
                            promise.SetValue(client);
                        });
            }

            return promise.GetFuture();
        }

    private:
        const ServiceMetaManager::Ptr                                   mServiceMetaManager;

        const std::vector<AsyncConnector::ConnectOptions::ConnectOptionFunc>    mConnectOptions;
        gayrpc::utils::ClientBuilder                                            mRpcClientBuilder;

        std::mutex                                                      mOrleansConnectionGrard;
        std::map<IPAddr, orleans::core::OrleansServiceClient::PTR>      mOrleans;
    };

} }