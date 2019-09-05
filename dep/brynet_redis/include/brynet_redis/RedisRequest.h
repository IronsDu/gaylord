#ifndef _REDIS_PROTOCOL_H
#define _REDIS_PROTOCOL_H

#include <stdlib.h>
#include <string.h>
#include <string>
#include <stdint.h>
#include <vector>
#include <unordered_map>

class RedisProtocolRequest
{
public:
    RedisProtocolRequest()
    {
        mArgc = 0;
    }

    virtual ~RedisProtocolRequest() = default;

    void    init()
    {
        mArgc = 0;
        mResult.clear();
    }

    template<typename Arg1, typename... Args>
    void            writev(const Arg1& arg1, const Args&... args)
    {
        this->operator<<(arg1);
        writev(args...);
    }

    std::string    endl() const
    {
        std::string tmp;
        tmp.push_back('*');
        tmp += std::to_string(mArgc);
        tmp += "\r\n";
        tmp += mResult;
        return tmp;
    }

    void            appendBinary(const char* buffer, size_t len)
    {
        addStr(buffer, len);
    }

    void            writev()
    {
    }

private:
    RedisProtocolRequest & operator << (const std::vector<std::string> &keys)
    {
        for (auto& v : keys)
        {
            addStr(v);
        }
        return *this;
    }

    RedisProtocolRequest & operator << (const std::unordered_map<std::string, std::string> &kvs)
    {
        for (auto& it : kvs)
        {
            addStr(it.first);
            addStr(it.second);
        }
        return *this;
    }

    RedisProtocolRequest & operator << (const int64_t &v)
    {
        addStr(std::to_string(v));
        return *this;
    }
    RedisProtocolRequest & operator << (const char* const &v)
    {
        addStr(v, strlen(v));
        return *this;
    }
    RedisProtocolRequest & operator << (const std::string &v)
    {
        addStr(v);
        return *this;
    }

private:
    void addStr(const std::string& arg)
    {
        addStr(arg.c_str(), arg.size());
    }

    void addStr(const char* buffer, size_t len)
    {
        mResult.push_back('$');
        mResult += (std::to_string(len));
        mResult += "\r\n";
        mResult.append(buffer, len);
        mResult += "\r\n";
        mArgc += 1;
    }

private:
    int             mArgc;
    std::string     mResult;
};

#endif
