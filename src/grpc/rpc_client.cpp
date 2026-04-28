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
#include "sisl/grpc/rpc_client.hpp"
#include "utils.hpp"

SISL_LOGGING_DECL(grpc_server)

namespace sisl {

GrpcBaseClient::GrpcBaseClient(const std::string& server_addr, const std::string& target_domain,
                               const std::string& ssl_cert) :
        GrpcBaseClient::GrpcBaseClient(server_addr, nullptr, target_domain, ssl_cert, 0, 0) {}

GrpcBaseClient::GrpcBaseClient(const std::string& server_addr,
                               const std::shared_ptr< sisl::GrpcTokenClient >& token_client,
                               const std::string& target_domain, const std::string& ssl_cert,
                               const int max_receive_msg_size, const int max_send_msg_size) :
        m_server_addr(server_addr),
        m_target_domain(target_domain),
        m_ssl_cert(ssl_cert),
        m_token_client(token_client),
        m_max_receive_msg_size(max_receive_msg_size),
        m_max_send_msg_size(max_send_msg_size) {}

void GrpcBaseClient::init() {
    ::grpc::SslCredentialsOptions ssl_opts;
    ::grpc::ChannelArguments channel_args;
    channel_args.SetMaxReceiveMessageSize(-1);
    if (m_max_receive_msg_size != 0) {
        LOGINFO("Setting max receive message size to {}", m_max_receive_msg_size);
        channel_args.SetMaxReceiveMessageSize(m_max_receive_msg_size);
    }
    if (m_max_send_msg_size != 0) {
        LOGINFO("Setting max send message size to {}", m_max_send_msg_size);
        channel_args.SetMaxSendMessageSize(m_max_send_msg_size);
    }

    if (!m_ssl_cert.empty()) {
        if (load_ssl_cert(m_ssl_cert, ssl_opts.pem_root_certs)) {
            if (!m_target_domain.empty()) { channel_args.SetSslTargetNameOverride(m_target_domain); }
            m_channel = ::grpc::CreateCustomChannel(m_server_addr, ::grpc::SslCredentials(ssl_opts), channel_args);
        } else {
            throw std::runtime_error("Unable to load ssl certification for grpc client");
        }
    } else {
        m_channel = ::grpc::CreateCustomChannel(m_server_addr, ::grpc::InsecureChannelCredentials(), channel_args);
    }
}

bool GrpcBaseClient::load_ssl_cert(const std::string& ssl_cert, std::string& content) {
    return get_file_contents(ssl_cert, content);
}

bool GrpcBaseClient::is_connection_ready() const {
    return (m_channel->GetState(true) == grpc_connectivity_state::GRPC_CHANNEL_READY);
}

std::mutex GrpcAsyncClientWorker::s_workers_mtx;
std::unordered_map< std::string, GrpcAsyncClientWorker::UPtr > GrpcAsyncClientWorker::s_workers;

GrpcAsyncClientWorker::~GrpcAsyncClientWorker() { shutdown(); }

void GrpcAsyncClientWorker::shutdown() {
    if (m_state == ClientState::RUNNING) {
        m_cq.Shutdown();
        m_state = ClientState::SHUTTING_DOWN;
        for (auto& thr : m_threads) {
            thr.join();
        }
        m_state = ClientState::TERMINATED;
    }
}

void GrpcAsyncClientWorker::run(uint32_t num_threads) {
    LOGMSG_ASSERT_EQ(ClientState::INIT, m_state);
    if (num_threads == 0) { throw std::invalid_argument("Need at least one worker thread"); }
    for (uint32_t i = 0u; i < num_threads; ++i) {
        m_threads.emplace_back(&GrpcAsyncClientWorker::client_loop, this);
    }
    m_state = ClientState::RUNNING;
}

void GrpcAsyncClientWorker::client_loop() {
#ifdef _POSIX_THREADS
#ifndef __APPLE__
    auto tname = std::string("grpc_client").substr(0, 15);
    pthread_setname_np(pthread_self(), tname.c_str());
#endif
#endif

    void* tag;
    bool ok = false;
    while (m_cq.Next(&tag, &ok)) {
        // For client-side unary calls, ok is always true even when the server is unreachable;
        // the real error is in GrpcUnaryOpState::m_status, checked in on_complete().
        // op_state lifetime is managed by the stdexec connect/start machinery — do NOT delete.
        static_cast< GrpcCqTag* >(tag)->on_complete(ok);
    }
}

void GrpcAsyncClientWorker::create_worker(const std::string& name, int num_threads) {
    std::lock_guard< std::mutex > lock(s_workers_mtx);
    if (s_workers.find(name) != s_workers.end()) { return; }
    auto worker = std::make_unique< GrpcAsyncClientWorker >();
    worker->run(num_threads);
    s_workers.insert(std::make_pair(name, std::move(worker)));
}

GrpcAsyncClientWorker* GrpcAsyncClientWorker::get_worker(const std::string& name) {
    std::lock_guard< std::mutex > lock(s_workers_mtx);
    auto it = s_workers.find(name);
    if (it == s_workers.end()) { return nullptr; }
    return it->second.get();
}

void GrpcAsyncClientWorker::shutdown_all() {
    std::lock_guard< std::mutex > lock(s_workers_mtx);
    for (auto& it : s_workers) {
        it.second->shutdown();
        // CompletionQueue must be destroyed before gRPC library internals.
        it.second.reset();
    }
    s_workers.clear();
}

} // namespace sisl
