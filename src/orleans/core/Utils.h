#pragma once

#include <string>
#include <sstream>
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

        static std::vector<std::string> split(const std::string& s, char delimiter)
        {
            std::vector<std::string> tokens;
            std::string token;
            std::istringstream tokenStream(s);
            while (std::getline(tokenStream, token, delimiter))
            {
                tokens.push_back(token);
            }
            return tokens;
        }

        static IPAddr GetIPAddrFromString(std::string addr)
        {
            std::vector<std::string> strs = Utils::split(addr, ':');
            assert(strs.size() == 2);
            return std::make_pair(strs[0], std::stoll(strs[1]));
        }
    };

} }