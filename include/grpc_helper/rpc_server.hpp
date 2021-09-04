#include <vector>
#include <memory>
#include <string>

#include <boost/core/noncopyable.hpp>
#include <grpcpp/completion_queue.h>
#include <sds_logging/logging.h>
#include <utility/enum.hpp>
#include "rpc_call.hpp"

namespace grpc_helper {

using rpc_thread_start_cb_t = std::function< void(uint32_t) >;

ENUM(ServerState, uint8_t, VOID, INITED, RUNNING, SHUTTING_DOWN, TERMINATED);

class GrpcServer : private boost::noncopyable {
    friend class RPCHelper;

public:
    GrpcServer(const std::string& listen_addr, uint32_t threads, const std::string& ssl_key,
               const std::string& ssl_cert);
    virtual ~GrpcServer();

    /**
     * Create a new GrpcServer instance and initialize it.
     */
    static GrpcServer* make(const std::string& listen_addr, uint32_t threads = 1, const std::string& ssl_key = "",
                            const std::string& ssl_cert = "");

    void run(const rpc_thread_start_cb_t& thread_start_cb = nullptr);
    void shutdown();
    bool is_terminated() const { return m_state.load(std::memory_order_acquire) == ServerState::TERMINATED; }

    template < typename ServiceT >
    bool register_async_service() {
        DEBUG_ASSERT_EQ(ServerState::INITED, m_state, "register service in non-INITED state");

        auto name = ServiceT::service_full_name();
        if (m_services.find(name) != m_services.end()) {
            LOGMSG_ASSERT(false, "Duplicate register async service");
            return false;
        }

        auto svc = new typename ServiceT::AsyncService();
        m_builder.RegisterService(svc);
        m_services.insert({name, svc});

        return true;
    }

    template < typename ServiceT, typename ReqT, typename RespT, bool streaming = false >
    bool register_rpc(const std::string& name, const request_call_cb_t& request_call_cb,
                      const rpc_handler_cb_t& rpc_handler, const rpc_completed_cb_t& done_handler = nullptr) {
        DEBUG_ASSERT_EQ(ServerState::RUNNING, m_state, "register service in non-INITED state");

        auto it = m_services.find(ServiceT::service_full_name());
        if (it == m_services.end()) {
            LOGMSG_ASSERT(false, "RPC registration attempted before service is registered");
            return false;
        }

        auto svc = static_cast< typename ServiceT::AsyncService* >(it->second);

        size_t rpc_idx;
        {
            std::unique_lock lg(m_rpc_registry_mtx);
            rpc_idx = m_rpc_registry.size();
            m_rpc_registry.emplace_back(new RpcStaticInfo< ServiceT, ReqT, RespT, false >(
                this, *svc, request_call_cb, rpc_handler, done_handler, rpc_idx, name));

            // Register one call per cq.
            for (auto i = 0u; i < m_cqs.size(); ++i) {
                auto rpc_call = RpcData< ServiceT, ReqT, RespT, false >::make(
                    (rpc_call_static_info_t*)m_rpc_registry[rpc_idx].get(), i);
                rpc_call->enqueue_call_request(*m_cqs[i]);
            }
        }

        return true;
    }

private:
    void handle_rpcs(uint32_t thread_num, const rpc_thread_start_cb_t& thread_start_cb);

private:
    std::atomic< ServerState > m_state{ServerState::VOID};
    uint32_t m_num_threads{0};
    ::grpc::ServerBuilder m_builder;

    std::unique_ptr< ::grpc::Server > m_server;
    std::vector< std::shared_ptr< std::thread > > m_threads;
    std::vector< std::unique_ptr< ::grpc::ServerCompletionQueue > > m_cqs;

    std::unordered_map< const char*, ::grpc::Service* > m_services;
    std::mutex m_rpc_registry_mtx;
    std::vector< std::unique_ptr< RpcStaticInfoBase > > m_rpc_registry;
};
} // namespace grpc_helper
