#pragma once

#include <memory>
#include <brynet/utils/NonCopyable.h>
#include <gayrpc/utils/UtilsWrapper.h>
#include <absl/strings/str_split.h>
#include <orleans/core/ServiceMetaManager.h>
#include <orleans/core/orleans_service.gayrpc.h>

namespace orleans { namespace core {

    class ClientOrleansRuntime : public brynet::utils::NonCopyable, public std::enable_shared_from_this<ClientOrleansRuntime>
    {
    public:
        using Ptr = std::shared_ptr< ClientOrleansRuntime>;
        using OrleansConnectionCreatedCallback = std::function<void(orleans::core::OrleansServiceClient::PTR)>;

    public:
        virtual ~ClientOrleansRuntime() = default;

        explicit ClientOrleansRuntime(ServiceMetaManager::Ptr metaManager)
            :
            mServiceMetaManager(metaManager),
            mTCPService(brynet::net::TcpService::Create()),
            mTCPConnector(brynet::net::AsyncConnector::Create())
        {
            mTCPService->startWorkerThread(1);
            mTCPConnector->startWorkerThread();
        }

        // 获取grain
        template<typename GrainType>
        auto takeGrain(std::string grainID)
        {
            const auto grainTypeName = GrainType::GetServiceTypeName();
            const auto grainUniqueName = grainTypeName + ":" + grainID;

            auto sharedThis = shared_from_this();
            auto grainRpcHandlerManager = std::make_shared<gayrpc::core::RpcTypeHandleManager>();

            auto grain = GrainType::Create(grainRpcHandlerManager,
                [=](const gayrpc::core::RpcMeta& meta,
                    const google::protobuf::Message& message,
                    const gayrpc::core::UnaryHandler& next,
                    InterceptorContextType context)
                {
                    return next(meta, message, std::move(context));
                },
                [=, serviceMetaManager = mServiceMetaManager](const gayrpc::core::RpcMeta& meta,
                    const google::protobuf::Message& message,
                    const gayrpc::core::UnaryHandler& next,
                    InterceptorContextType context)
                {
                    auto seq_id = meta.request_info().sequence_id();
                    auto timeoutSecond = meta.request_info().timeout();
                    // outboundInterceptor
                    // 处理业务层RPC Client的输出(即Request)
                    // 将业务RPC包裹在 OrleansRequest
                    orleans::core::OrleansRequest request;
                    request.set_grain_type(grainTypeName);
                    request.set_grain_unique_name(grainUniqueName);
                    *request.mutable_meta() = meta;
                    request.set_body(message.SerializeAsString());

                    std::shared_ptr<google::protobuf::Message> p(message.New());
                    p->CopyFrom(message);

                    // 尝试创建到grain所在节点的RPC
                    auto caller = [=](std::string addr) {
                        std::vector<std::string> strs = absl::StrSplit(addr, ":");
                        assert(strs.size() == 2);
                        sharedThis->asyncOrleansConnectionCreated(std::make_pair(strs[0],
                            std::stoll(strs[1])),
                            [=, context = std::move(context)](orleans::core::OrleansServiceClient::PTR orleanClient) {
                            auto timeout = std::chrono::seconds(timeoutSecond > 0 ? timeoutSecond : 10);
                            orleanClient->Request(request, 
                                [=, context = std::move(context)](const orleans::core::OrleansResponse& response, const gayrpc::core::RpcError& error) {
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
                                            grainRpcHandlerManager->handleRpcMsg(errorMeta, "", std::move(context));
                                        }
                                        catch(...)
                                        { }
                                    }
                                    else
                                    {
                                        InterceptorContextType context;
                                        grainRpcHandlerManager->handleRpcMsg(response.meta(), response.body(), context);
                                    }
                                    // 每次调用成功都处理此grain地址状态
                                    serviceMetaManager->processAddrStatus(grainUniqueName, addr, true);
                                },
                                timeout,
                                [=]() {
                                    // 超时处理此grain地址状态
                                    serviceMetaManager->processAddrStatus(grainUniqueName, addr,false);
                                });
                            next(meta, *p, std::move(context));
                        });
                    };
                    serviceMetaManager->queryOrCreateGrainAddr(grainTypeName, grainUniqueName, caller);
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
            const auto grainUniqueName = grainTypeName + ":" + grainID;
            auto sharedThis = shared_from_this();

            auto caller = [sharedThis, grainTypeName, grainUniqueName](std::string addr) {
                if (addr.empty()) {
                    return;
                }
                std::vector<std::string> strs = absl::StrSplit(addr, ":");
                assert(strs.size() == 2);
                sharedThis->asyncOrleansConnectionCreated(std::make_pair(strs[0],
                    std::stoll(strs[1])),
                    [=](orleans::core::OrleansServiceClient::PTR orleanClient) {
                        orleans::core::OrleansReleaseRequest request;
                        request.set_grain_type(grainTypeName);
                        request.set_grain_unique_name(grainUniqueName);
                        orleanClient->Release(request,
                            [](const orleans::core::OrleansReleaseResponse&, const gayrpc::core::RpcError&) {
                            },
                            std::chrono::seconds(10),
                                []() {
                            });
                    });
            };
            mServiceMetaManager->queryGrainAddr(grainTypeName, grainUniqueName, caller);
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

        void asyncOrleansConnectionCreated(IPAddr addr, OrleansConnectionCreatedCallback callback)
        {
            auto orleans = findOrleanConnection(addr);
            if (orleans != nullptr)
            {
                // 直接执行回调
                callback(orleans);
            }
            else
            {
                // TODO::链接配置
                // 如果当前没有到节点的链接则异步创建
                gayrpc::utils::AsyncCreateRpcClient<orleans::core::OrleansServiceClient>(
                    mTCPService,
                    mTCPConnector,
                    addr.first, addr.second, std::chrono::seconds(10),
                    nullptr,
                    nullptr,
                    nullptr,
                    [=](orleans::core::OrleansServiceClient::PTR client) {
                        // RPC对象创建成功则执行回调
                        callback(client);
                    },
                    []() {}, 1024 * 1024, std::chrono::seconds(10));
            }
        }

    private:
        const ServiceMetaManager::Ptr                                   mServiceMetaManager;
        const brynet::net::TcpService::Ptr                              mTCPService;
        const brynet::net::AsyncConnector::Ptr                          mTCPConnector;

        std::mutex                                                      mOrleansConnectionGrard;
        std::map<IPAddr, orleans::core::OrleansServiceClient::PTR>      mOrleans;
    };

} }