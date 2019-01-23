#pragma once

#include <memory>
#include <brynet/utils/NonCopyable.h>
#include <absl/strings/str_split.h>
#include <orleans/core/ServiceMetaManager.h>

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

        template<typename T>
        auto takeGrain(std::string grainName)
        {
            auto sharedThis = shared_from_this();

            auto grainTypeName = T::GetServiceTypeName();
            auto grainRpcHandlerManager = std::make_shared<gayrpc::core::RpcTypeHandleManager>();

            auto grain = T::Create(grainRpcHandlerManager,
                [=](const gayrpc::core::RpcMeta& meta,
                    const google::protobuf::Message& message,
                    const gayrpc::core::UnaryHandler& next,
                    InterceptorContextType context)
                {
                    return next(meta, message, std::move(context));
                },
                [=](const gayrpc::core::RpcMeta& meta,
                    const google::protobuf::Message& message,
                    const gayrpc::core::UnaryHandler& next,
                    InterceptorContextType context)
                {
                    // outboundInterceptor
                    // 处理业务层RPC Client的输出(即Request)
                    // 将业务RPC包裹在 OrleansRequest
                    orleans::core::OrleansRequest request;
                    request.set_grain_type(grainTypeName);
                    request.set_grain_name(grainName);
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
                            orleanClient->Request(request, [=, context = std::move(context)](const orleans::core::OrleansResponse& response, const gayrpc::core::RpcError&) {
                                // 将收到的response交给用户层RPC
                                InterceptorContextType context;
                                grainRpcHandlerManager->handleRpcMsg(response.meta(), response.body(), context);
                            });
                            next(meta, *p, std::move(context));
                        });
                    };
                    mServiceMetaManager->QueryGrainAddr(grainTypeName, grainName, caller);
                });

            return grain;
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
        const brynet::net::TcpService::PTR                              mTCPService;
        const brynet::net::AsyncConnector::PTR                          mTCPConnector;

        std::mutex                                                      mOrleansConnectionGrard;
        std::map<IPAddr, orleans::core::OrleansServiceClient::PTR>      mOrleans;
    };

} }