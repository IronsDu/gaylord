#pragma once

#include <memory>
#include <functional>
#include <string>

#include <orleans/core/CoreType.h>

namespace orleans { namespace core {
    
    class ServiceMetaManager
    {
    public:
        using Ptr = std::shared_ptr< ServiceMetaManager>;
        using QueryGrainCompleted = std::function<void(std::string addr)>;

        virtual ~ServiceMetaManager() = default;

        // 注册某类型服务可由addr地址处理
        virtual void    registerGrain(GrainTypeName, std::string addr) = 0;
        // 查询某类型某名称的grain的地址
        virtual void    queryGrainAddr(GrainTypeName grainTypeName, std::string grainUniqueName, QueryGrainCompleted) = 0;
        // 查询或创建某类型某名称的grain的地址
        virtual void    queryOrCreateGrainAddr(GrainTypeName grainTypeName, std::string grainUniqueName, QueryGrainCompleted) = 0;
        // 处理某个grain地址的当前状态(以更新本地grain缓存）
        virtual void    processAddrStatus(std::string grainUniqueName, std::string addr, bool isGood) = 0;
        // 更新当前本地grain缓存
        virtual void    updateGrairAddrList() = 0;
        // 存活某grain
        virtual void    activeGrain(std::string grainUniqueName) = 0;
    };

} }