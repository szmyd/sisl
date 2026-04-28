/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <exec/task.hpp>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "sisl/grpc/generic_service.hpp"
#include "sisl/grpc/rpc_client.hpp"
#include "sisl/grpc/rpc_server.hpp"
#include "grpc_helper_test.grpc.pb.h"

using namespace sisl;
using namespace ::grpc_helper_test;

#define MAX_GRPC_RECV_SIZE (64 * 1024 * 1024)

#ifdef __SANITIZE_THREAD__
static constexpr uint32_t k_rpc_deadline{30};
#else
// Original test serialized about half of the generic calls via blocking futures, limiting large-payload
// concurrency to ~1 at a time. The coroutine version fires all calls concurrently, so we need more
// headroom for the scheduler to work through the full 64 MB payload calls.
static constexpr uint32_t k_rpc_deadline{10};
#endif

// ---------------------------------------------------------------------------
// Helpers shared between client and server
// ---------------------------------------------------------------------------
static constexpr std::array< const char, 62 > alphanum{
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
    'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

static std::string gen_random_string(size_t len) {
    std::string str;
    static thread_local std::random_device rd{};
    static thread_local std::default_random_engine re{rd()};
    std::uniform_int_distribution< size_t > rand_char{0, alphanum.size() - 1};
    for (size_t i = 0; i < len; ++i) {
        str += alphanum[rand_char(re)];
    }
    str += '\0';
    return str;
}

// Payload used for all generic RPC calls — generated once, sub-stringed per call.
static const std::string GENERIC_CLIENT_MESSAGE{gen_random_string(MAX_GRPC_RECV_SIZE)};
static const std::string GENERIC_METHOD{"SendData"};

struct DataMessage {
    int m_seqno{0};
    std::string m_buf;

    DataMessage() = default;
    DataMessage(int n, const std::string& buf) : m_seqno{n}, m_buf{buf} {}

    static int num_digits(int n) {
        int ret = 0;
        for (; n > 0; ret++) {
            n /= 10;
        }
        return ret;
    }

    void serialize_to_string(std::string& out) const {
        out.append(std::to_string(num_digits(m_seqno)));
        out.append(std::to_string(m_seqno));
        out.append(m_buf);
    }

    void deserialize_from_string(const std::string& s) {
        int nd = s[0] - '0';
        m_seqno = std::stoi(s.substr(1, nd));
        m_buf = s.substr(1 + nd);
    }
};

static void serialize_to_byte_buffer(grpc::ByteBuffer& buf, const DataMessage& msg) {
    std::string s;
    msg.serialize_to_string(s);
    buf.Clear();
    grpc::Slice slice(s);
    grpc::ByteBuffer tmp(&slice, 1);
    buf.Swap(&tmp);
}

static void deserialize_from_buffer(const grpc::ByteBuffer& buf, DataMessage& msg) {
    std::vector< grpc::Slice > slices;
    (void)buf.Dump(&slices);
    std::string s;
    s.reserve(buf.Length());
    for (auto const& sl : slices) {
        s.append(reinterpret_cast< const char* >(sl.begin()), sl.size());
    }
    msg.deserialize_from_string(s);
}

static void deserialize_from_blob(sisl::io_blob const& blob, DataMessage& msg) {
    std::string s{reinterpret_cast< const char* >(blob.cbytes()), blob.size()};
    msg.deserialize_from_string(s);
}

// ---------------------------------------------------------------------------
// TestClient
// ---------------------------------------------------------------------------
class TestClient {
public:
    static constexpr int GRPC_CALL_COUNT = 400;
    const std::string WORKER_NAME{"Worker-1"};

    exec::task< void > do_echo(GrpcAsyncClient::AsyncStub< EchoService >* stub, int i) {
        EchoRequest req;
        req.set_message(std::to_string(i));
        try {
            auto reply =
                co_await grpc_as_exception(stub->call(req, &EchoService::StubInterface::AsyncEcho, k_rpc_deadline));
            LOGDEBUGMOD(grpc_server, "echo request {} reply {}", req.message(), reply.message());
            RELEASE_ASSERT_EQ(req.message(), reply.message(), "echo reply mismatch for request {}", i);
        } catch (const GrpcStatusException& e) { RELEASE_ASSERT(false, "echo request {} failed: {}", i, e.what()); }
    }

    exec::task< void > do_ping(GrpcAsyncClient::AsyncStub< PingService >* stub, int i) {
        PingRequest req;
        req.set_seqno(i);
        try {
            auto reply =
                co_await grpc_as_exception(stub->call(req, &PingService::StubInterface::AsyncPing, k_rpc_deadline));
            LOGDEBUGMOD(grpc_server, "ping request {} reply {}", req.seqno(), reply.seqno());
            RELEASE_ASSERT_EQ(req.seqno(), reply.seqno(), "ping reply mismatch for request {}", i);
        } catch (const GrpcStatusException& e) { RELEASE_ASSERT(false, "ping request {} failed: {}", i, e.what()); }
    }

    exec::task< void > do_generic(GrpcAsyncClient::GenericAsyncStub* stub, int i) {
        // Cycle through a range of payload sizes to stress gRPC framing paths.
        static constexpr int mess_sizes[] = {16, 64, 64 * 1024, 16 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024 - 1024};
        static thread_local std::mt19937 rng{std::random_device{}()};
        static std::uniform_int_distribution< int > pick{0, (int)(std::size(mess_sizes)) - 1};

        int size = mess_sizes[pick(rng)];
        DataMessage req{i, GENERIC_CLIENT_MESSAGE.substr(0, size)};
        grpc::ByteBuffer cli_buf;
        serialize_to_byte_buffer(cli_buf, req);

        try {
            auto reply_buf = co_await grpc_as_exception(stub->call(cli_buf, GENERIC_METHOD, k_rpc_deadline));
            GenericClientResponse resp{reply_buf};
            DataMessage svr_msg;
            deserialize_from_blob(resp.response_blob(), svr_msg);
            RELEASE_ASSERT_EQ(req.m_seqno, svr_msg.m_seqno, "generic seqno mismatch for request {}", i);
            RELEASE_ASSERT_EQ(req.m_buf, svr_msg.m_buf, "generic data mismatch for request {}", i);
        } catch (const GrpcStatusException& e) {
            RELEASE_ASSERT(false, "generic request {} failed: {}", i, e.what());
        } catch (const std::exception& e) {
            RELEASE_ASSERT(false, "generic request {} deserialization error: {}", i, e.what());
        }
    }

    void run(const std::string& server_address) {
        auto client = std::make_unique< GrpcAsyncClient >(server_address, "", "");
        client->init();
        GrpcAsyncClientWorker::create_worker(WORKER_NAME, 4);

        auto echo_stub = client->make_stub< EchoService >(WORKER_NAME);
        auto ping_stub = client->make_stub< PingService >(WORKER_NAME);
        auto generic_stub = client->make_generic_stub(WORKER_NAME);

        exec::static_thread_pool pool{4};
        auto sched = pool.get_scheduler();
        exec::async_scope scope;

        for (int i = 1; i <= GRPC_CALL_COUNT; ++i) {
            if ((i % 2) == 0) {
                scope.spawn(stdexec::starts_on(sched, do_echo(echo_stub.get(), i)));
            } else if ((i % 3) == 0) {
                scope.spawn(stdexec::starts_on(sched, do_ping(ping_stub.get(), i)));
            } else {
                scope.spawn(stdexec::starts_on(sched, do_generic(generic_stub.get(), i)));
            }
        }

        stdexec::sync_wait(scope.on_empty());
        GrpcAsyncClientWorker::shutdown_all();
    }
};

// ---------------------------------------------------------------------------
// TestServer
// ---------------------------------------------------------------------------
class TestServer {
public:
    class EchoServiceImpl final {
        std::atomic< uint32_t > num_calls{0};

    public:
        void register_service(GrpcServer* server) {
            RELEASE_ASSERT(server->register_async_service< EchoService >(), "Failed to Register EchoService");
        }

        void register_rpcs(GrpcServer* server) {
            LOGINFO("register echo rpc");
            auto res = server->register_rpc< EchoService, EchoRequest, EchoReply, false >(
                "Echo", &EchoService::AsyncService::RequestEcho,
                [this](const AsyncRpcDataPtr< EchoService, EchoRequest, EchoReply >& rpc_data) {
                    if ((++num_calls % 2) == 0) {
                        LOGDEBUGMOD(grpc_server, "respond async echo request {}", rpc_data->request().message());
                        std::thread([rpc = rpc_data] {
                            rpc->response().set_message(rpc->request().message());
                            rpc->send_response();
                        }).detach();
                        return false;
                    }
                    LOGDEBUGMOD(grpc_server, "respond sync echo request {}", rpc_data->request().message());
                    rpc_data->response().set_message(rpc_data->request().message());
                    return true;
                });
            RELEASE_ASSERT(res, "register echo rpc failed");
        }
    };

    class PingServiceImpl final {
        std::atomic< uint32_t > num_calls{0};

    public:
        void register_service(GrpcServer* server) {
            RELEASE_ASSERT(server->register_async_service< PingService >(), "Failed to Register PingService");
        }

        void register_rpcs(GrpcServer* server) {
            LOGINFO("register ping rpc");
            auto res = server->register_rpc< PingService, PingRequest, PingReply, false >(
                "Ping", &PingService::AsyncService::RequestPing,
                [this](const AsyncRpcDataPtr< PingService, PingRequest, PingReply >& rpc_data) {
                    if ((++num_calls % 2) == 0) {
                        LOGDEBUGMOD(grpc_server, "respond async ping request {}", rpc_data->request().seqno());
                        std::thread([rpc = rpc_data] {
                            rpc->response().set_seqno(rpc->request().seqno());
                            rpc->send_response();
                        }).detach();
                        return false;
                    }
                    LOGDEBUGMOD(grpc_server, "respond sync ping request {}", rpc_data->request().seqno());
                    rpc_data->response().set_seqno(rpc_data->request().seqno());
                    return true;
                });
            RELEASE_ASSERT(res, "register ping rpc failed");
        }
    };

    class GenericServiceImpl final {
        std::atomic< uint32_t > num_calls{0};
        std::atomic< uint32_t > num_completions{0};

        // send_response(io_blob_list_t) uses STATIC_SLICE — the caller must keep blob data alive
        // until after the gRPC write completes (on_buf_write fires).  We stash the blob in the
        // RPC context, which is destroyed in ~GenericRpcData() after on_request_completed.
        struct BlobContext : public GenericRpcContextBase {
            sisl::io_blob blob;
            explicit BlobContext(sisl::io_blob b) : blob{b} {}
            ~BlobContext() override { blob.buf_free(); }
        };

        // Fills rpc->response() with the echoed DataMessage.  Does NOT call send_response()
        // so it works for both sync (infrastructure sends) and async (thread sends) paths.
        static void prepare_byte_buffer_response(const boost::intrusive_ptr< GenericRpcData >& rpc) {
            DataMessage msg;
            deserialize_from_buffer(rpc->request(), msg);
            serialize_to_byte_buffer(rpc->response(), msg);
        }

        // Exercises send_response(io_blob_list_t).  Always calls send_response() itself
        // so it is only called from the async thread path.
        static void echo_via_io_blob(const boost::intrusive_ptr< GenericRpcData >& rpc) {
            DataMessage msg;
            deserialize_from_buffer(rpc->request(), msg);
            std::string s;
            msg.serialize_to_string(s);
            sisl::io_blob blob{static_cast< uint32_t >(s.size()), 0};
            std::memcpy(blob.bytes(), s.data(), s.size());
            // Keep blob alive until after the write completes (context freed in ~GenericRpcData).
            rpc->set_context(std::make_unique< BlobContext >(blob));
            rpc->send_response(io_blob_list_t{blob});
        }

    public:
        void register_service(GrpcServer* server) {
            RELEASE_ASSERT(server->register_async_generic_service(), "Failed to Register GenericService");
        }

        void register_rpcs(GrpcServer* server) {
            LOGINFO("register generic rpc");
            auto res =
                server->register_generic_rpc(GENERIC_METHOD, [this](boost::intrusive_ptr< GenericRpcData >& rpc_data) {
                    rpc_data->set_comp_cb([this](boost::intrusive_ptr< GenericRpcData >&) { ++num_completions; });
                    auto call_n = ++num_calls;
                    if ((call_n % 2) == 0) {
                        LOGDEBUGMOD(grpc_server, "respond async generic request, call_num {}", call_n);
                        std::thread([call_n, rpc = rpc_data] {
                            if ((call_n % 3) == 0) {
                                echo_via_io_blob(rpc);
                            } else {
                                prepare_byte_buffer_response(rpc);
                                rpc->send_response();
                            }
                        }).detach();
                        return false;
                    }
                    // Sync path: prepare response, return true so infrastructure calls send_response().
                    prepare_byte_buffer_response(rpc_data);
                    return true;
                });
            RELEASE_ASSERT(res, "register generic rpc failed");
        }

        bool check_counters() const {
            if (num_calls != num_completions) {
                LOGERROR("num_calls={} num_completions={} — mismatch", num_calls.load(), num_completions.load());
                return false;
            }
            return true;
        }
    };

    void start(const std::string& server_address) {
        LOGINFO("Starting echo/ping/generic server on {}", server_address);
        m_grpc_server = GrpcServer::make(server_address, 4, "", "", MAX_GRPC_RECV_SIZE);
        m_echo_impl.register_service(m_grpc_server);
        m_ping_impl.register_service(m_grpc_server);
        m_generic_impl.register_service(m_grpc_server);
        m_grpc_server->run();
        m_echo_impl.register_rpcs(m_grpc_server);
        m_ping_impl.register_rpcs(m_grpc_server);
        m_generic_impl.register_rpcs(m_grpc_server);
        LOGINFO("Server listening on {}", server_address);
    }

    void shutdown() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        RELEASE_ASSERT(m_generic_impl.check_counters(), "generic RPC call/completion counter mismatch");
        LOGINFO("Shutting down grpc server");
        m_grpc_server->shutdown();
        delete m_grpc_server;
    }

private:
    GrpcServer* m_grpc_server{nullptr};
    EchoServiceImpl m_echo_impl;
    PingServiceImpl m_ping_impl;
    GenericServiceImpl m_generic_impl;
};

SISL_LOGGING_INIT(logging, grpc_server)
SISL_OPTIONS_ENABLE(logging)

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging)
    sisl::logging::SetLogger("async_client");

    TestServer server;
    const std::string server_address{"0.0.0.0:50052"};
    server.start(server_address);

    TestClient client;
    client.run(server_address);

    server.shutdown();
    return 0;
}
