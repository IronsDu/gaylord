#pragma once

#include <string>
#include <absl/strings/str_split.h>
#include <orleans/core/CoreType.h>

namespace orleans { namespace core {

    class Utils
    {
    public:
        static std::string MakeGrainUniqueName(GrainTypeName grainTypeName, std::string grainID)
        {
            return grainTypeName + ":" + grainID;
        }

        static std::string MakeIpAddrString(std::string ip, int port)
        {
            return ip + ":" + std::to_string(port);;
        }

        static IPAddr GetIPAddrFromString(std::string addr)
        {
            std::vector<std::string> strs = absl::StrSplit(addr, ":");
            assert(strs.size() == 2);
            return std::make_pair(strs[0], std::stoll(strs[1]));
        }
    };

} }