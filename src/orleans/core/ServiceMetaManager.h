#pragma once

#include <memory>
#include <functional>
#include <string>
#include <chrono>
#include <optional>

#include <orleans/core/CoreType.h>
#include <ananas/future/Future.h>

namespace orleans { namespace core {
    
    class ServiceMetaManager
    {
    public:
        using Ptr = std::shared_ptr< ServiceMetaManager>;

        virtual ~ServiceMetaManager() = default;

        // 注册某类型服务可由addr地址处理
        virtual ananas::Future<bool>    
            registerGrain(GrainTypeName, std::string addr, std::chrono::milliseconds timeout) = 0;

        // 查询某类型某名称的grain的地址
        virtual ananas::Future<std::string>
            queryGrainAddr(GrainTypeName grainTypeName, std::string grainUniqueName, std::chrono::milliseconds timeout) = 0;

        // 查询或创建某类型某名称的grain的地址
        virtual ananas::Future<std::string>
            queryOrCreateGrainAddr(GrainTypeName grainTypeName, std::string grainUniqueName, std::chrono::milliseconds timeout) = 0;

        // 存活某grain
        virtual ananas::Future<bool>
            activeGrain(std::string grainUniqueName, std::chrono::seconds expire, std::chrono::milliseconds timeout) = 0;

        // 强制将某grain创建到某地址
        virtual ananas::Future<bool>
            createGrainByAddr(std::string grainUniqueName, std::string addr, std::chrono::milliseconds timeout) = 0;

        // 处理某个grain地址的当前状态(以更新本地grain缓存）
        virtual void    processAddrStatus(std::string grainUniqueName, std::string addr, bool isGood) = 0;

        // 更新当前本地grain缓存
        virtual void    updateGrairAddrList() = 0;
    };

} }