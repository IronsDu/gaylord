#pragma once

#include <string>
#include <sstream>
#include <iterator>
#include <brynet/net/TCPService.h>
#include <brynet/net/Connector.h>
#include <brynet/net/Wrapper.h>

#include <brynet_redis/RedisRequest.h>
#include <brynet_redis/RedisParse.h>
#include <ananas/future/Future.h>

using namespace brynet;
using namespace brynet::net;

class RedisReply
{
public:
    RedisReply(const redisReply* reply)
        :
        mReply(reply)
    {}

    virtual ~RedisReply() = default;

    bool    isValid() const noexcept
    {
        return mReply != nullptr;
    }

    bool    isOk() const noexcept
    {
        return !isError();
    }

    bool    isError() const noexcept
    {
        return mReply->type == REDIS_REPLY_ERROR;
    }

    bool    isNull() const noexcept
    {
        return mReply->type == REDIS_REPLY_NIL;
    }

    bool    isArray() const noexcept
    {
        return mReply->type == REDIS_REPLY_ARRAY;
    }

    bool    isInteger() const noexcept
    {
        return mReply->type == REDIS_REPLY_INTEGER;
    }

    bool    isString() const noexcept
    {
        return mReply->type == REDIS_REPLY_STRING;
    }

    bool    isStatus() const noexcept
    {
        return mReply->type == REDIS_REPLY_STATUS;
    }

    operator int64_t() const
    {
        expect({ REDIS_REPLY_INTEGER });
        return mReply->integer;
    }

    operator std::string() const
    {
        expect({ REDIS_REPLY_STRING, REDIS_REPLY_STATUS, REDIS_REPLY_ERROR });
        return std::string(mReply->str, mReply->len);
    }

    operator std::vector<RedisReply>() const
    {
        expect({ REDIS_REPLY_ARRAY });
        std::vector< RedisReply> result;
        result.reserve(mReply->elements);
        for (size_t i = 0; i < mReply->elements; i++)
        {
            result.push_back(RedisReply(mReply->element[i]));
        }
        return result;
    }

private:
    void    expect(std::vector< int32_t> expectTypes) const
    {
        if(!expectTypes.empty() && 
            std::find(expectTypes.begin(), expectTypes.end(), mReply->type) == expectTypes.end())
        {
            std::ostringstream oss;
            std::copy(expectTypes.begin(), expectTypes.end() - 1,
                std::ostream_iterator<int32_t>(oss, ","));
            oss << expectTypes.back();

            throw std::runtime_error("type error, expect:[" 
                + oss.str()
                + "], but current type is:" 
                + std::to_string(mReply->type));
        }
    }

private:
    const redisReply* mReply;
};

struct RedisConnectConfig
{
    std::string ip;
    int port;
    size_t  recvBufferSize = 1024 * 1024;
    std::chrono::milliseconds connectTimeout = std::chrono::seconds(10);
};

class RedisClient : public std::enable_shared_from_this<RedisClient>
{
public:
    using ReplyCallback = std::function<void(RedisReply)>;
    using TimeoutCallback = std::function<void()>;

    using Ptr = std::shared_ptr<RedisClient>;

    struct WaitReplyCallback
    {
        using Ptr = std::shared_ptr< WaitReplyCallback>;

        ReplyCallback       callback;
        std::atomic_bool    executed = false;
        brynet::timer::Timer::WeakPtr   timer;
    };

    RedisClient(TcpService::Ptr tcpService,
        AsyncConnector::Ptr connector)
        :
        mTcpService(tcpService),
        mConnector(connector),
        mConnecting(false)
    {
    }

    virtual ~RedisClient() = default;

    ananas::Future<bool>    asyncConnect(RedisConnectConfig config)
    {
        ananas::Promise<bool> promise;

        std::lock_guard<std::mutex> lck(mConnectingGuard);
        if (mConnecting)
        {
            throw std::runtime_error("already connecting");
        }
        mConnecting = true;

        auto self = shared_from_this();
        auto enterCallback = [=](const TcpConnection::Ptr& session) {
            self->onConnected(session, [=]() mutable {
                    promise.SetValue(true);
                }, []() {
                });
        };
        auto wrapperFailedCallback = [=]() {
            self->onConnectFailed([=]() mutable {
                    promise.SetValue(false);
                });
        };

        wrapper::ConnectionBuilder connectionBuilder;
        connectionBuilder.configureService(mTcpService)
            .configureConnector(mConnector)
            .configureConnectionOptions({
                    brynet::net::TcpService::AddSocketOption::AddEnterCallback(enterCallback),
                    brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(config.recvBufferSize)
                })
            .configureConnectOptions({
                    AsyncConnector::ConnectOptions::WithAddr(config.ip, config.port),
                    AsyncConnector::ConnectOptions::WithTimeout(config.connectTimeout),
                    AsyncConnector::ConnectOptions::WithFailedCallback(std::move(wrapperFailedCallback)),
                    AsyncConnector::ConnectOptions::AddProcessTcpSocketCallback([](TcpSocket& socket) {
                        socket.setNodelay();
                    })
                })
            .asyncConnect();
        return promise.GetFuture();
    }

    ananas::Future<RedisReply>    asyncRequest(std::string request,
        std::chrono::milliseconds timeout)
    {
        ananas::Promise<RedisReply> promise;
        _asyncRequest(request,
            [promise](RedisReply r) mutable {
                promise.SetValue(r);
            },
            timeout,
            [promise]() mutable {
                RedisReply r(nullptr);
                promise.SetValue(r);
            });
        return promise.GetFuture();
    }

    ananas::Future<RedisReply>    asyncRequest(const std::vector<std::string>& request,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest r;
        r.writev(request);
        return asyncRequest(r.endl(), timeout);
    }

private:
    void    _asyncRequest(std::string request, 
        ReplyCallback callback, 
        std::chrono::milliseconds timetout, 
        TimeoutCallback timeoutCallback)
    {
        std::lock_guard<std::mutex> lck(mReplyCallbackListGuard);
        WaitReplyCallback::Ptr wrapperCallback = std::make_shared<WaitReplyCallback>();
        wrapperCallback->callback = callback;
        wrapperCallback->executed = false;

        mReplyCallbackList.push_back(wrapperCallback);
        mConnection->send(request.c_str(), request.size());
        auto eventLoop = mConnection->getEventLoop();
        wrapperCallback->timer= eventLoop->runAfter(timetout, [=]() {
                if (!wrapperCallback->executed.exchange(true))
                {
                    timeoutCallback();
                }
            });
    }

    void    _asyncRequest(const std::vector<std::string>& request,
        ReplyCallback callback,
        std::chrono::milliseconds timetout,
        TimeoutCallback timeoutCallback)
    {
        RedisProtocolRequest r;
        r.writev(request);
        _asyncRequest(r.endl(), callback, timetout, timeoutCallback);
    }

private:
    void    onConnected(const TcpConnection::Ptr& session, 
        std::function<void()> connectCallback, 
        std::function<void()> closedCallback)
    {
        auto self = shared_from_this();
        mConnection = session;
        mConnection->setDisConnectCallback([=](TcpConnection::Ptr) {
                self->onClosed(closedCallback);
            });
        mConnection->setDataCallback([self](const char* buffer, size_t len) {
                return self->onMsg(buffer, len);
            });
        connectCallback();
    }

    void    onConnectFailed(std::function<void()> failedCallback)
    {
        {
            std::lock_guard<std::mutex> lck(mConnectingGuard);
            assert(mConnecting);
            mConnecting = false;
        }
        failedCallback();
    }

    void    onClosed(std::function<void()> closedCallback)
    {
        {
            std::lock_guard<std::mutex> lck(mConnectingGuard);
            assert(mConnecting);
            mConnecting = false;
        }
        closedCallback();
    }

    size_t  onMsg(const char* buffer, size_t len)
    {
        size_t totalLen = 0;

        char* parseEndPos = (char*)buffer;
        char* parseStartPos = parseEndPos;

        while (totalLen < len)
        {
            if (mRedisParse == nullptr)
            {
                mRedisParse = std::shared_ptr<parse_tree>(parse_tree_new(), [](parse_tree* parse) {
                        parse_tree_del(parse);
                    });
            }

            const int parseRet = parse(mRedisParse.get(), &parseEndPos, (char*)buffer + len);
            totalLen += (parseEndPos - parseStartPos);

            if (parseRet == REDIS_OK)
            {
                processReply(mRedisParse);
                parseStartPos = parseEndPos;
                mRedisParse = nullptr;
            }
            else if (parseRet == REDIS_RETRY)
            {
                break;
            }
            else
            {
                break;
            }
        }

        return totalLen;
    }

    void processReply(const std::shared_ptr<parse_tree>& redisReplyParseTree)
    {
        WaitReplyCallback::Ptr wrapperCallback;
        {
            std::lock_guard<std::mutex> lck(mReplyCallbackListGuard);
            assert(!mReplyCallbackList.empty());
            if (!mReplyCallbackList.empty())
            {
                wrapperCallback = mReplyCallbackList.front();
                mReplyCallbackList.pop_front();
            }
        }
        if (wrapperCallback != nullptr && !wrapperCallback->executed.exchange(true))
        {
            auto timer = wrapperCallback->timer.lock();
            if (timer != nullptr)
            {
                timer->cancel();
            }
            wrapperCallback->callback(RedisReply(redisReplyParseTree->reply));
        }
    }

private:
    const TcpService::Ptr               mTcpService;
    const AsyncConnector::Ptr           mConnector;

    bool                                mConnecting;
    std::mutex                          mConnectingGuard;
    brynet::net::TcpConnectionPtr       mConnection;

    std::shared_ptr<parse_tree>         mRedisParse;

    std::list< WaitReplyCallback::Ptr>  mReplyCallbackList;
    std::mutex                          mReplyCallbackListGuard;
};

class SuperRedisClient : public RedisClient
{
public:
    using Ptr = std::shared_ptr< SuperRedisClient>;

    using RedisClient::RedisClient;

    auto    set(const std::string& key, 
        const std::string& value,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest request;
        request.writev("SET", key, value);
        return asyncRequest(request.endl(), timeout);
    }

    auto    get(const std::string& key,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest request;
        request.writev("GET", key);
        return asyncRequest(request.endl(), timeout);
    }

    auto    lpush(const std::string& key,
        const std::vector<std::string>& value,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest request;
        request.writev("LPUSH", key, value);
        return asyncRequest(request.endl(), timeout);
    }

    auto    lrange(const std::string& key,
        int64_t start,
        int64_t end,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest request;
        request.writev("LRANGE", key, start, end);
        return asyncRequest(request.endl(), timeout);
    }

    auto    setnx(const std::string& key,
        const std::string& value,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest request;
        request.writev("SETNX", key, value);
        return asyncRequest(request.endl(), timeout);
    }

    auto    expire(const std::string& key,
        int64_t seconds,
        std::chrono::milliseconds timeout)
    {
        RedisProtocolRequest request;
        request.writev("EXPIRE", key, seconds);
        return asyncRequest(request.endl(), timeout);
    }
};