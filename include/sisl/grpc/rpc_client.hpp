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
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <boost/core/noncopyable.hpp>
#include <fmt/format.h>
#include <grpc/support/log.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <stdexec/execution.hpp>

#include <sisl/auth_manager/token_client.hpp>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/utility/enum.hpp>
#include <sisl/utility/obj_life_counter.hpp>

namespace grpc {
inline auto format_as(StatusCode s) { return fmt::underlying(s); }
} // namespace grpc

namespace sisl {

// ---------------------------------------------------------------------------
// GrpcCqTag — base for all CompletionQueue tags
//
// The void* registered with grpc::CompletionQueue::Finish() is always a
// GrpcCqTag*.  The drain loop in GrpcAsyncClientWorker dispatches through
// this vtable, then leaves lifetime management to the concrete subclass.
// ---------------------------------------------------------------------------
struct GrpcCqTag {
    virtual ~GrpcCqTag() = default;
    virtual void on_complete(bool ok) noexcept = 0;
};

// ---------------------------------------------------------------------------
// GrpcStatusException — carries a grpc::Status as a std::exception.
//
// Use grpc_as_exception() to adapt a GrpcUnarySender for co_await inside
// exec::task<>, which requires std::exception_ptr on its error channel.
// ---------------------------------------------------------------------------
struct GrpcStatusException : public std::exception {
    explicit GrpcStatusException(::grpc::Status s) noexcept : m_status{std::move(s)}, m_msg{m_status.error_message()} {}
    const char* what() const noexcept override { return m_msg.c_str(); }
    ::grpc::Status m_status;
    std::string m_msg;
};

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------
template < typename ReqT, typename RespT >
class GrpcUnarySender;

template < typename ReqT, typename RespT, typename Receiver >
class GrpcUnaryOpState;

// ---------------------------------------------------------------------------
// GrpcUnarySender<ReqT, RespT>
//
// A P2300 sender representing a single in-flight gRPC unary call.
// Completion signatures:
//   set_value_t(RespT)        — RPC succeeded
//   set_error_t(grpc::Status) — RPC failed
//   set_stopped_t()           — stop was requested before or during the call
//
// Obtain one via AsyncStub<ServiceT>::call().
// ---------------------------------------------------------------------------
template < typename ReqT, typename RespT >
class GrpcUnarySender {
public:
    using sender_concept = stdexec::sender_t;
    using completion_signatures =
        stdexec::completion_signatures< stdexec::set_value_t(RespT), stdexec::set_error_t(::grpc::Status),
                                        stdexec::set_stopped_t() >;

    using ResponseReader = ::grpc::ClientAsyncResponseReaderInterface< RespT >;
    // Abstracts the typed stub + method-pointer so the sender carries only two template params.
    using StartFn = std::function< std::unique_ptr< ResponseReader >(::grpc::ClientContext*, const ReqT&,
                                                                     ::grpc::CompletionQueue*) >;

    GrpcUnarySender(ReqT req, StartFn fn, uint32_t deadline,
                    std::vector< std::pair< std::string, std::string > > metadata, ::grpc::CompletionQueue* cq,
                    std::shared_ptr< sisl::GrpcTokenClient > token_client) :
            m_req{std::move(req)},
            m_start_fn{std::move(fn)},
            m_deadline{deadline},
            m_metadata{std::move(metadata)},
            m_cq{cq},
            m_token_client{std::move(token_client)} {}

    template < stdexec::receiver_of< completion_signatures > Receiver >
    auto connect(Receiver rcvr) && -> GrpcUnaryOpState< ReqT, RespT, Receiver >;

private:
    ReqT m_req;
    StartFn m_start_fn;
    uint32_t m_deadline;
    std::vector< std::pair< std::string, std::string > > m_metadata;
    ::grpc::CompletionQueue* m_cq;
    std::shared_ptr< sisl::GrpcTokenClient > m_token_client;

    template < typename, typename, typename >
    friend class GrpcUnaryOpState;
};

// ---------------------------------------------------------------------------
// GrpcUnaryOpState<ReqT, RespT, Receiver>
//
// The operation_state produced by GrpcUnarySender::connect().  Its address is
// passed to grpc::Finish() as the void* CQ tag and MUST remain stable after
// start() is called — it is therefore non-copyable and non-movable.
// ---------------------------------------------------------------------------
template < typename ReqT, typename RespT, typename Receiver >
class GrpcUnaryOpState : public GrpcCqTag {
public:
    using operation_state_concept = stdexec::operation_state_t;

    explicit GrpcUnaryOpState(GrpcUnarySender< ReqT, RespT >&& s, Receiver rcvr) :
            m_req{std::move(s.m_req)},
            m_start_fn{std::move(s.m_start_fn)},
            m_deadline{s.m_deadline},
            m_metadata{std::move(s.m_metadata)},
            m_cq{s.m_cq},
            m_token_client{std::move(s.m_token_client)},
            m_receiver{std::move(rcvr)} {}

    GrpcUnaryOpState(const GrpcUnaryOpState&) = delete;
    GrpcUnaryOpState(GrpcUnaryOpState&&) = delete;
    GrpcUnaryOpState& operator=(const GrpcUnaryOpState&) = delete;
    GrpcUnaryOpState& operator=(GrpcUnaryOpState&&) = delete;

    void start() & noexcept {
        if (stdexec::get_stop_token(stdexec::get_env(m_receiver)).stop_requested()) {
            stdexec::set_stopped(std::move(m_receiver));
            return;
        }
        m_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(m_deadline));
        for (auto const& [k, v] : m_metadata) {
            m_context.AddMetadata(k, v);
        }
        if (m_token_client) {
            m_context.AddMetadata(m_token_client->get_auth_header_key(), m_token_client->get_token());
        }
        m_reader = m_start_fn(&m_context, m_req, m_cq);
        m_reader->Finish(&m_reply, &m_status, static_cast< GrpcCqTag* >(this));
    }

    // Called from the GrpcAsyncClientWorker drain thread when the CQ event fires.
    void on_complete([[maybe_unused]] bool ok) noexcept override {
        if (stdexec::get_stop_token(stdexec::get_env(m_receiver)).stop_requested()) {
            stdexec::set_stopped(std::move(m_receiver));
        } else if (!m_status.ok()) {
            stdexec::set_error(std::move(m_receiver), std::move(m_status));
        } else {
            stdexec::set_value(std::move(m_receiver), std::move(m_reply));
        }
    }

private:
    // Declaration order must match the constructor initializer list to satisfy -Wreorder.
    ReqT m_req;
    typename GrpcUnarySender< ReqT, RespT >::StartFn m_start_fn;
    uint32_t m_deadline;
    std::vector< std::pair< std::string, std::string > > m_metadata;
    ::grpc::CompletionQueue* m_cq;
    std::shared_ptr< sisl::GrpcTokenClient > m_token_client;
    Receiver m_receiver;
    // Default-constructed; not in the initializer list.
    RespT m_reply{};
    ::grpc::ClientContext m_context;
    ::grpc::Status m_status;
    std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< RespT > > m_reader;
};

// connect() is defined here (after GrpcUnaryOpState is complete) to satisfy the forward decl above.
template < typename ReqT, typename RespT >
template < stdexec::receiver_of< typename GrpcUnarySender< ReqT, RespT >::completion_signatures > Receiver >
auto GrpcUnarySender< ReqT, RespT >::connect(Receiver rcvr) && -> GrpcUnaryOpState< ReqT, RespT, Receiver > {
    return GrpcUnaryOpState< ReqT, RespT, Receiver >{std::move(*this), std::move(rcvr)};
}

// ---------------------------------------------------------------------------
// grpc_as_exception(sender)
//
// Adapts a GrpcUnarySender for co_await inside exec::task<>.
// Converts set_error(grpc::Status) → set_error(exception_ptr) so the task's
// try/catch machinery can handle gRPC failures uniformly.
// ---------------------------------------------------------------------------
template < typename Sender >
auto grpc_as_exception(Sender&& s) {
    return std::forward< Sender >(s) | stdexec::let_error([](::grpc::Status status) noexcept {
               return stdexec::just_error(std::make_exception_ptr(GrpcStatusException{std::move(status)}));
           });
}

// ---------------------------------------------------------------------------
// GenericClientResponse — ByteBuffer response as a contiguous sisl::io_blob.
//
// Converts a grpc::ByteBuffer (possibly multi-slice) into a single allocation.
// Uses shared ownership so copies are cheap; both original and copy refer to
// the same underlying buffer.
// ---------------------------------------------------------------------------
class GenericClientResponse {
public:
    GenericClientResponse() = default;

    explicit GenericClientResponse(const ::grpc::ByteBuffer& buf) {
        std::vector< ::grpc::Slice > slices;
        (void)buf.Dump(&slices);
        size_t total = 0;
        for (auto const& s : slices) {
            total += s.size();
        }
        if (total > 0) {
            m_buf = sisl::make_byte_array(static_cast< uint32_t >(total), 0);
            uint8_t* dst = m_buf->bytes();
            for (auto const& s : slices) {
                std::memcpy(dst, s.begin(), s.size());
                dst += s.size();
            }
        }
    }

    sisl::io_blob response_blob() const {
        if (!m_buf) { return sisl::io_blob{}; }
        return sisl::io_blob{m_buf->cbytes(), m_buf->size(), m_buf->is_aligned()};
    }

private:
    sisl::byte_array m_buf;
};

// ---------------------------------------------------------------------------
// GrpcBaseClient — manages the channel and SSL/token state
// ---------------------------------------------------------------------------
class GrpcBaseClient {
protected:
    const std::string m_server_addr;
    const std::string m_target_domain;
    const std::string m_ssl_cert;

    std::shared_ptr< ::grpc::ChannelInterface > m_channel;
    std::shared_ptr< sisl::GrpcTokenClient > m_token_client;

    int m_max_receive_msg_size{0};
    int m_max_send_msg_size{0};

public:
    GrpcBaseClient(const std::string& server_addr, const std::string& target_domain = "",
                   const std::string& ssl_cert = "");
    GrpcBaseClient(const std::string& server_addr, const std::shared_ptr< sisl::GrpcTokenClient >& token_client,
                   const std::string& target_domain = "", const std::string& ssl_cert = "",
                   int max_receive_msg_size = 0, int max_send_msg_size = 0);
    virtual ~GrpcBaseClient() = default;
    virtual bool is_connection_ready() const;
    virtual void init();

private:
    virtual bool load_ssl_cert(const std::string& ssl_cert, std::string& content);
};

class GrpcSyncClient : public GrpcBaseClient {
public:
    using GrpcBaseClient::GrpcBaseClient;

    template < typename ServiceT >
    std::unique_ptr< typename ServiceT::StubInterface > MakeStub() {
        return ServiceT::NewStub(m_channel);
    }
};

ENUM(ClientState, uint8_t, VOID, INIT, RUNNING, SHUTTING_DOWN, TERMINATED)

// ---------------------------------------------------------------------------
// GrpcAsyncClientWorker
//
// Owns a grpc::CompletionQueue and N drain threads.  All GrpcUnarySender
// operations submitted through stubs created from the same worker share
// this CQ.  The drain loop dispatches via GrpcCqTag::on_complete(); it does
// NOT delete the tag — that is the operation_state's responsibility.
// ---------------------------------------------------------------------------
class GrpcAsyncClientWorker final {
public:
    using UPtr = std::unique_ptr< GrpcAsyncClientWorker >;

    GrpcAsyncClientWorker() = default;
    ~GrpcAsyncClientWorker();

    void run(uint32_t num_threads);

    ::grpc::CompletionQueue& cq() { return m_cq; }

    static void create_worker(const std::string& name, int num_threads);
    static GrpcAsyncClientWorker* get_worker(const std::string& name);
    static void shutdown_all();

private:
    void shutdown();
    void client_loop();

    static std::mutex s_workers_mtx;
    static std::unordered_map< std::string, GrpcAsyncClientWorker::UPtr > s_workers;

    ClientState m_state{ClientState::INIT};
    ::grpc::CompletionQueue m_cq;
    std::vector< std::thread > m_threads;
};

inline constexpr std::string_view request_id_header{"request_id"};

// ---------------------------------------------------------------------------
// GrpcAsyncClient — creates typed stubs backed by a GrpcAsyncClientWorker
// ---------------------------------------------------------------------------
class GrpcAsyncClient : public GrpcBaseClient {
public:
    template < typename ServiceT >
    using StubPtr = std::unique_ptr< typename ServiceT::StubInterface >;

    GrpcAsyncClient(const std::string& server_addr, const std::shared_ptr< sisl::GrpcTokenClient > token_client,
                    const std::string& target_domain = "", const std::string& ssl_cert = "",
                    int max_receive_msg_size = 0, int max_send_msg_size = 0) :
            GrpcBaseClient(server_addr, token_client, target_domain, ssl_cert, max_receive_msg_size,
                           max_send_msg_size) {}

    GrpcAsyncClient(const std::string& server_addr, const std::string& target_domain = "",
                    const std::string& ssl_cert = "") :
            GrpcAsyncClient(server_addr, nullptr, target_domain, ssl_cert, 0, 0) {}

    ~GrpcAsyncClient() override = default;

    // -----------------------------------------------------------------------
    // AsyncStub<ServiceT>
    //
    // Wraps a generated gRPC stub and a worker.  Use call() to obtain a
    // GrpcUnarySender for each unary RPC.
    // -----------------------------------------------------------------------
    template < typename ServiceT >
    struct AsyncStub {
        using UPtr = std::unique_ptr< AsyncStub >;
        using stub_t = typename ServiceT::StubInterface;

        template < typename RespT >
        using unary_call_return_t = std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< RespT > >;

        template < typename ReqT, typename RespT >
        using unary_call_t = unary_call_return_t< RespT > (stub_t::*)(::grpc::ClientContext*, const ReqT&,
                                                                      ::grpc::CompletionQueue*);

        AsyncStub(StubPtr< ServiceT > stub, GrpcAsyncClientWorker* worker,
                  std::shared_ptr< sisl::GrpcTokenClient > token_client) :
                m_stub{std::move(stub)}, m_worker{worker}, m_token_client{std::move(token_client)} {}

        // Returns a sender that, when started, performs the gRPC unary call and
        // delivers the response through the stdexec completion protocol.
        template < typename ReqT, typename RespT >
        auto call(const ReqT& request, const unary_call_t< ReqT, RespT >& method, uint32_t deadline,
                  std::span< const std::pair< std::string, std::string > > metadata = {})
            -> GrpcUnarySender< ReqT, RespT > {
            typename GrpcUnarySender< ReqT, RespT >::StartFn fn = [stub = m_stub.get(),
                                                                   method](::grpc::ClientContext* ctx, const ReqT& req,
                                                                           ::grpc::CompletionQueue* cq_ptr) {
                return (stub->*method)(ctx, req, cq_ptr);
            };
            return GrpcUnarySender< ReqT, RespT >{request, std::move(fn), deadline, {metadata.begin(), metadata.end()},
                                                  cq(),    m_token_client};
        }

        const StubPtr< ServiceT >& stub() { return m_stub; }
        ::grpc::CompletionQueue* cq() { return &m_worker->cq(); }

        StubPtr< ServiceT > m_stub;
        GrpcAsyncClientWorker* m_worker;
        std::shared_ptr< sisl::GrpcTokenClient > m_token_client;
    };

    // -----------------------------------------------------------------------
    // GenericAsyncStub
    //
    // Wraps a grpc::GenericStub and a worker.  Use call() to obtain a
    // GrpcUnarySender<ByteBuffer, ByteBuffer> for unary byte-buffer RPCs.
    // PrepareUnaryCall handles gRPC framing internally — same one-CQ-event
    // path as the typed stub, no multi-step streaming protocol needed.
    // -----------------------------------------------------------------------
    struct GenericAsyncStub {
        using UPtr = std::unique_ptr< GenericAsyncStub >;

        GenericAsyncStub(std::unique_ptr< ::grpc::GenericStub > stub, GrpcAsyncClientWorker* worker,
                         std::shared_ptr< sisl::GrpcTokenClient > token_client) :
                m_stub{std::move(stub)}, m_worker{worker}, m_token_client{std::move(token_client)} {}

        GrpcUnarySender< ::grpc::ByteBuffer, ::grpc::ByteBuffer >
        call(const ::grpc::ByteBuffer& req, const std::string& method, uint32_t deadline,
             std::span< const std::pair< std::string, std::string > > metadata = {}) {
            // PrepareUnaryCall is the "prepare" variant — StartCall() must be called
            // before Finish(). The typed AsyncXxx stubs call StartCall internally,
            // so we mirror that here to keep GrpcUnaryOpState uniform.
            typename GrpcUnarySender< ::grpc::ByteBuffer, ::grpc::ByteBuffer >::StartFn fn =
                [stub = m_stub.get(), method](::grpc::ClientContext* ctx, const ::grpc::ByteBuffer& r,
                                              ::grpc::CompletionQueue* cq_ptr)
                -> std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::grpc::ByteBuffer > > {
                auto reader = stub->PrepareUnaryCall(ctx, method, r, cq_ptr);
                reader->StartCall();
                return std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::grpc::ByteBuffer > >(
                    reader.release());
            };
            return GrpcUnarySender< ::grpc::ByteBuffer, ::grpc::ByteBuffer >{
                req, std::move(fn), deadline, {metadata.begin(), metadata.end()}, cq(), m_token_client};
        }

        ::grpc::CompletionQueue* cq() { return &m_worker->cq(); }

        std::unique_ptr< ::grpc::GenericStub > m_stub;
        GrpcAsyncClientWorker* m_worker;
        std::shared_ptr< sisl::GrpcTokenClient > m_token_client;
    };

    template < typename T, typename... Ts >
    static auto make(Ts&&... params) {
        return std::make_unique< T >(std::forward< Ts >(params)...);
    }

    template < typename ServiceT >
    auto make_stub(const std::string& worker) {
        auto* w = GrpcAsyncClientWorker::get_worker(worker);
        if (w == nullptr) { throw std::runtime_error("worker thread not available"); }
        return std::make_unique< AsyncStub< ServiceT > >(ServiceT::NewStub(m_channel), w, m_token_client);
    }

    auto make_generic_stub(const std::string& worker) {
        auto* w = GrpcAsyncClientWorker::get_worker(worker);
        if (w == nullptr) { throw std::runtime_error("worker thread not available"); }
        return std::make_unique< GenericAsyncStub >(std::make_unique< ::grpc::GenericStub >(m_channel), w,
                                                    m_token_client);
    }
};

} // namespace sisl
