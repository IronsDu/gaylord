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
        virtual void    Register(GrainTypeName, std::string addr) = 0;
        // 查询某类型某名称的grain的地址
        virtual void    QueryGrainAddr(GrainTypeName grainTypeName, std::string grainName, QueryGrainCompleted) = 0;
    };

} }