#include "scenario.hpp"
#include "atlasfs/socket.hpp"
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <sys/resource.h>
#include <sys/utsname.h>

namespace {
using namespace atlasfs;
using Clock = std::chrono::steady_clock;
struct Config {
    std::size_t iterations{30}, objects{32}, bytes{65536};
    std::uint32_t chunk{16384}, replicas{2}, nodes{4}, workers{1};
    std::uint64_t seed{42};
    std::string suite{"all"}, output, label;
};
std::string quote(std::string_view value) {
    std::string result{"\""};
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') { result += '\\'; result += static_cast<char>(c); }
        else if (c < 32) { const char* hex = "0123456789abcdef"; result += "\\u00"; result += hex[c >> 4]; result += hex[c & 15]; }
        else result += static_cast<char>(c);
    }
    return result + '"';
}
std::uint64_t number(const std::string& text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("parameters must be unsigned integers");
    return std::stoull(text);
}
Config parse(int argc, char** argv) {
    Config c;
    for (int n = 1; n < argc; n += 2) {
        const std::string key = argv[n];
        if (key == "--help") {
            std::cout << "--iterations N --objects N --bytes N --chunk N --replicas N --nodes N --workers N --seed N\n"
                         "--suite all|local|distributed|fault --output file.json --label environment-label\n";
            std::exit(0);
        }
        if (n + 1 >= argc) throw std::invalid_argument("option requires a value");
        const std::string value = argv[n + 1];
        if (key == "--output") c.output = value;
        else if (key == "--label") c.label = value;
        else if (key == "--suite") c.suite = value;
        else if (key == "--seed") c.seed = number(value);
        else {
            const auto v = number(value);
            if (v > 4194304) throw std::invalid_argument("numeric parameter exceeds bound");
            if (key == "--iterations") c.iterations = v;
            else if (key == "--objects") c.objects = v;
            else if (key == "--bytes") c.bytes = v;
            else if (key == "--chunk") c.chunk = static_cast<std::uint32_t>(v);
            else if (key == "--replicas") c.replicas = static_cast<std::uint32_t>(v);
            else if (key == "--nodes") c.nodes = static_cast<std::uint32_t>(v);
            else if (key == "--workers") c.workers = static_cast<std::uint32_t>(v);
            else throw std::invalid_argument("unknown option");
        }
    }
    if (!c.iterations || c.iterations > 10000 || !c.objects || c.objects > 10000 ||
        !c.bytes || !c.chunk || !c.nodes || c.nodes > 16 || !c.replicas || c.replicas > c.nodes ||
        !c.workers || c.workers > 16 || (c.bytes + c.chunk - 1) / c.chunk > storage::kMaxChunkCount ||
        (c.suite != "all" && c.suite != "local" && c.suite != "distributed" && c.suite != "fault"))
        throw std::invalid_argument("unsupported workload bounds");
    return c;
}
double cpu_seconds() {
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) return -1;
    return static_cast<double>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
        static_cast<double>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}
std::uint64_t disk_bytes(const std::filesystem::path& root, bool temporary = false) {
    std::uint64_t result{};
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        const auto name = entry.path().filename().string();
        if (!entry.is_regular_file() || entry.path().extension() == ".pem") continue;
        if (!temporary || name.ends_with(".tmp") || name.starts_with(".atlasfs-tmp-")) result += entry.file_size();
    }
    return result;
}
struct Outcome { std::uint64_t bytes{}; bool rejected{}, unknown{}, timeout{}; double recovery_ms{-1}; };
struct Metric {
    std::string name;
    std::size_t operations{}, errors{}, rejected{}, unknown{}, timeouts{};
    std::uint64_t bytes{}, disk{}, temporary{};
    std::size_t object_bytes{}; std::uint32_t chunks{}, nodes{}, replicas{}, workers{};
    double wall{}, cpu{};
    std::vector<double> latencies;
    std::vector<double> recovery_latencies;
    std::string failure;
    double percentile(double p) const {
        auto values = latencies; std::sort(values.begin(), values.end());
        return values.empty() ? 0 : values[static_cast<std::size_t>(std::ceil(p * values.size())) - 1];
    }
};
class Harness {
public:
    explicit Harness(Config config) : config(std::move(config)) {}
    Config config;
    std::vector<Metric> results;
    template<class F> void measure(std::string name, std::size_t count, std::size_t bytes,
        std::uint32_t nodes, std::uint32_t replicas, const std::filesystem::path& root, F operation,
        std::uint32_t workers = 1) {
        Metric m; m.name = std::move(name); m.object_bytes = bytes;
        m.chunks = bytes ? static_cast<std::uint32_t>((bytes + config.chunk - 1) / config.chunk) : 0;
        m.nodes = nodes; m.replicas = replicas; m.workers = workers;
        std::mutex metrics_mutex;
        std::atomic_size_t next{0};
        auto work = [&] {
            for (;;) {
                const auto n = next.fetch_add(1); if (n >= count) break;
                const auto start = Clock::now(); Outcome outcome; std::string failure;
                try { outcome = operation(n); }
                catch (const std::exception& error) { failure = error.what(); }
                const auto latency = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                std::lock_guard lock(metrics_mutex);
                ++m.operations; m.latencies.push_back(latency); m.bytes += outcome.bytes;
                m.rejected += outcome.rejected; m.unknown += outcome.unknown; m.timeouts += outcome.timeout;
                if (outcome.recovery_ms >= 0) m.recovery_latencies.push_back(outcome.recovery_ms);
                if (!failure.empty()) { ++m.errors; if (m.failure.empty()) m.failure = "iteration=" + std::to_string(n) + " " + failure; }
            }
        };
        const auto cpu = cpu_seconds(); const auto start = Clock::now();
        if (workers == 1) work();
        else { std::vector<std::thread> threads; for (std::uint32_t n = 0; n < workers; ++n) threads.emplace_back(work);
            for (auto& thread : threads) thread.join(); }
        m.wall = std::chrono::duration<double>(Clock::now() - start).count();
        const auto end_cpu = cpu_seconds(); m.cpu = cpu < 0 || end_cpu < 0 ? -1 : end_cpu - cpu;
        m.disk = disk_bytes(root); m.temporary = disk_bytes(root, true);
        std::cerr << m.name << ": " << m.operations << " operations, " << m.errors << " unexpected errors\n";
        results.push_back(std::move(m));
    }
    void local() {
        test::Certificates temporary;
        storage::ObjectStore store(temporary.root() / "store", object_protocol::kMaxObjectDataSize, config.chunk);
        std::vector<std::size_t> sizes{1024, 65536, config.bytes};
        std::sort(sizes.begin(), sizes.end()); sizes.erase(std::unique(sizes.begin(), sizes.end()), sizes.end());
        for (const auto size : sizes) {
            const auto data = bench::payload(size, config.seed); const auto crc = integrity::crc32c(data);
            const auto prefix = "size-" + std::to_string(size) + '-';
            measure("local_write_" + std::to_string(size), config.iterations, size, 1, 1, temporary.root(), [&](auto n) {
                bench::require(store.put(prefix + std::to_string(n % config.objects), data, crc) == storage::StoreError::none, "local write");
                return Outcome{size};
            });
            measure("local_read_hot_" + std::to_string(size), config.iterations, size, 1, 1, temporary.root(), [&](auto) {
                const auto result = store.get(prefix + "0");
                bench::require(result.error == storage::StoreError::none && result.data == data && result.checksum == crc, "local checksum read");
                return Outcome{size};
            });
        }
        const auto data = bench::payload(config.bytes, config.seed); const auto crc = integrity::crc32c(data);
        for (std::size_t n = 0; n < config.objects; ++n)
            bench::require(store.put("mixed-" + std::to_string(n), data, crc) == storage::StoreError::none, "mixed seed");
        measure("local_mixed_75pct_reads", config.iterations, config.bytes, 1, 1, temporary.root(), [&](auto n) {
            const auto id = "mixed-" + std::to_string((n * 2654435761ULL + config.seed) % config.objects);
            if (n % 4 == 0) bench::require(store.put(id, data, crc) == storage::StoreError::none, "mixed write");
            else { const auto result = store.get(id); bench::require(result.error == storage::StoreError::none && result.data == data, "mixed read"); }
            return Outcome{config.bytes};
        });
        measure("storage_reopen_first_read", config.iterations, config.bytes, 1, 1, temporary.root(), [&](auto) {
            storage::ObjectStore reopened(temporary.root() / "store", object_protocol::kMaxObjectDataSize, config.chunk);
            const auto result = reopened.get("mixed-0");
            bench::require(result.error == storage::StoreError::none && result.data == data, "reopen read");
            return Outcome{config.bytes};
        });
    }
    void distributed() {
        bench::Cluster cluster(config.nodes, config.chunk);
        const auto data = bench::payload(config.bytes, config.seed);
        metadata::MetadataCoordinator placement(std::make_unique<metadata::RendezvousPlacement>(), config.replicas);
        for (const auto& node : cluster.nodes) bench::require(placement.register_node(bench::member(node.id)) == metadata::CoordinatorError::none, "register");
        auto manager = std::make_unique<replication::ReplicaManager>(cluster.clients());
        std::vector<metadata::PlacementPlan> plans;
        for (std::size_t n = 0; n < config.objects; ++n) {
            const auto value = bench::manifest(data, config.chunk, "generation-" + std::to_string(n));
            const auto result = placement.place_object({"object-" + std::to_string(n), value});
            bench::require(result.plan.has_value(), "placement"); plans.push_back(*result.plan);
        }
        measure("replicated_write_tls_epoch", config.objects, config.bytes, config.nodes, config.replicas, cluster.root(), [&](auto n) {
            auto guard = cluster.authority->authorize(cluster.token);
            bench::require(manager->put(plans[n], data).status == replication::ReplicationStatus::success, "replicated put object=" + plans[n].object_id);
            return Outcome{config.bytes};
        });
        std::mutex coordinator_queue;
        measure("replicated_read_serialized_queue", config.iterations, config.bytes, config.nodes, config.replicas, cluster.root(), [&](auto n) {
            std::lock_guard lock(coordinator_queue);
            const auto result = manager->get(plans[(n * 2654435761ULL + config.seed) % plans.size()]);
            bench::require(result.status == replication::ReplicationStatus::success && result.data == data, "replicated read");
            return Outcome{config.bytes};
        }, config.workers);
        measure("replicated_mixed_serialized_queue", config.iterations, config.bytes, config.nodes, config.replicas, cluster.root(), [&](auto n) {
            std::lock_guard lock(coordinator_queue);
            auto& plan = plans[(n * 2654435761ULL + config.seed) % plans.size()];
            auto guard = cluster.authority->authorize(cluster.token);
            if (n % 4 == 0) {
                const auto value = bench::manifest(data, config.chunk, "mixed-gen-" + std::to_string(n));
                const auto changed = placement.place_object({plan.object_id, value});
                bench::require(changed.plan.has_value(), "mixed placement");
                bench::require(manager->put(*changed.plan, data).status == replication::ReplicationStatus::success, "mixed replicated write");
                plan = *changed.plan;
            } else {
                const auto result = manager->get(plan);
                bench::require(result.status == replication::ReplicationStatus::success && result.data == data, "mixed replicated read");
            }
            return Outcome{config.bytes};
        }, config.workers);
        // Close the single active session before measuring new connection setup.
        manager.reset();
        cluster.nodes[0].client.reset();
        measure("tls_context_tcp_handshake", config.iterations, 0, 1, 0, cluster.root(), [&](auto) {
            net::TcpClient client(cluster.nodes[0].running->server.port(), cluster.coordinator,
                {cluster.nodes[0].id, net::PeerRole::storage});
            return Outcome{};
        });
        cluster.nodes[0].client = std::make_shared<replication::RemoteStorageNodeClient>(cluster.nodes[0].id,
            cluster.nodes[0].running->server.port(), cluster.coordinator, cluster.token);
        measure("tls_established_query_rpc", config.iterations, 0, 1, 0, cluster.root(), [&](auto) {
            bench::require(cluster.nodes[0].client->query_epoch().token == cluster.token, "established query");
            return Outcome{};
        });
        measure("epoch_duplicate_activation_rpc", config.iterations, 0, 1, 0, cluster.root(), [&](auto) {
            auto prior = cluster.token; prior.number = 0; prior.coordinator.clear(); prior.activation.clear();
            bench::require(cluster.nodes[0].client->activate_epoch(prior, cluster.token).status == epoch::Status::accepted, "duplicate activation");
            return Outcome{};
        });
    }
    void repair(bool recover, repair::RepairExecutionFaultPoint point) {
        bench::Cluster cluster(1, config.chunk);
        auto source = std::make_shared<replication::LocalStorageNode>("a", cluster.root() / "source",
            object_protocol::kMaxObjectDataSize, config.chunk);
        const auto data = bench::payload(std::min<std::size_t>(config.bytes, config.chunk), config.seed);
        auto catalog = std::make_unique<metadata::DurableReplicaCatalog>(cluster.root(), cluster.authority, cluster.token);
        auto plans = std::make_unique<repair::RepairPlanStore>(cluster.root());
        metadata::CoordinatorOperationStore history(cluster.root());
        const metadata::MembershipSnapshot members{10, {bench::member("a"), bench::member("b", false), bench::member("node-0")}};
        std::vector<std::pair<metadata::PlacementPlan, std::string>> pending;
        for (std::size_t n = 0; n < config.objects; ++n) {
            auto value = bench::placement("repair-" + std::to_string(n), bench::manifest(data, config.chunk, "repair-gen-" + std::to_string(n)), {"a", "b"});
            replication::ReplicaManager writer({source});
            auto seed = value; seed.replication_factor = 1; seed.chunks[0].replica_node_ids = {"a"};
            bench::require(writer.put(seed, data).status == replication::ReplicationStatus::success, "repair source seed");
            bench::seed_catalog(*catalog, value);
            pending.emplace_back(std::move(value), "");
        }
        const auto historical = metadata::make_coordinator_operation(pending[0].first, "history", 7);
        bench::require(history.create(historical) == metadata::CoordinatorRecordError::none, "history seed");
        const auto before = metadata::encode_coordinator_operation(*history.get("history"));
        measure(recover ? "repair_restart_reconcile_window_" + std::to_string(static_cast<int>(point)) : "remote_repair_verify_catalog_cas",
            std::min(config.iterations, config.objects), data.size(), 2, 2, cluster.root(), [&](auto n) {
                // Distinct objects are needed because repair journals cannot be silently recycled.
                auto& item = pending[n];
                const auto planned = repair::RepairPlanner{}.plan(item.first, members, catalog->snapshot());
                bench::require(planned.plans.size() == 1, "repair planning"); item.second = planned.plans[0].plan_id;
                bench::require(plans->create(planned.plans[0]) == repair::RepairStoreError::none, "repair journal create");
                repair::RepairExecutionResult executed;
                {
                    repair::RepairExecutor executor(*plans, *catalog, {source, cluster.nodes[0].client},
                        [=](auto at) { return recover && at == point; });
                    bench::require(executor.approve(item.second) == repair::RepairStoreError::none, "repair approve");
                    executed = executor.execute(item.second, item.first, members);
                }
                if (recover) {
                    bench::require(executed.error == repair::RepairExecutionError::recovery_interrupted, "interruption");
                    const auto original = plans->get(item.second)->execution_id;
                    const auto start = Clock::now();
                    auto reopened = std::make_unique<repair::RepairPlanStore>(cluster.root());
                    auto reopened_catalog = std::make_unique<metadata::DurableReplicaCatalog>(cluster.root(), cluster.authority, cluster.token);
                    repair::RepairExecutor recovery(*reopened, *reopened_catalog, {source, cluster.nodes[0].client});
                    bench::require(recovery.reconcile(item.second, item.first, members).state == repair::RepairPlanState::completed, "reconcile execution=" + original);
                    bench::require(reopened->get(item.second)->execution_id == original, "execution identity changed");
                    const auto ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                    bench::require(metadata::encode_coordinator_operation(*history.get("history")) == before, "recovery history changed");
                    // Do not keep a stale independent catalog snapshot as the next writer.
                    catalog = std::move(reopened_catalog); plans = std::move(reopened);
                    return Outcome{data.size(), false, false, false, ms};
                }
                bench::require(executed.state == repair::RepairPlanState::completed, "repair completion");
                bench::require(metadata::encode_coordinator_operation(*history.get("history")) == before, "history changed");
                return Outcome{data.size()};
            });
    }
    void faults();
    std::string json() const {
        utsname system{}; ::uname(&system); rusage usage{}; ::getrusage(RUSAGE_SELF, &usage);
        std::ostringstream out; out << std::setprecision(10);
        out << "{\n\"schema\":1,\"seed\":" << config.seed << ",\"iterations\":" << config.iterations
            << ",\"objects\":" << config.objects << ",\"bytes\":" << config.bytes << ",\"chunk\":" << config.chunk
            << ",\"nodes\":" << config.nodes << ",\"replicas\":" << config.replicas << ",\"workers\":" << config.workers
            << ",\"suite\":" << quote(config.suite) << ",\"label\":" << quote(config.label)
            << ",\"compiler\":" << quote(__VERSION__) << ",\"openssl\":" << quote(OpenSSL_version(OPENSSL_VERSION))
            << ",\"build_type\":" << quote(ATLASFS_BENCH_BUILD_TYPE) << ",\"source_commit\":" << quote(ATLASFS_BENCH_COMMIT)
            << ",\"checksum_source_sha256\":" << quote(ATLASFS_BENCH_CHECKSUM_HASH)
            << ",\"scratch_parent\":" << quote(std::filesystem::temp_directory_path().string())
            << ",\"os\":" << quote(std::string(system.sysname) + " " + system.release + " " + system.machine)
            << ",\"network\":\"loopback only; encrypted wire bytes not measured\",\"network_bytes\":null"
            << ",\"peak_rss_bytes\":";
        if (usage.ru_maxrss > 0) out << static_cast<std::uint64_t>(usage.ru_maxrss) * 1024; else out << "null";
        out << ",\"temporary_peak_bytes\":null,\"results\":[\n";
        bool first = true;
        for (const auto& m : results) {
            if (!first) out << ",\n";
            first = false;
            out << "{\"workload\":" << quote(m.name) << ",\"operations\":" << m.operations << ",\"unexpected_errors\":" << m.errors
                << ",\"expected_rejections\":" << m.rejected << ",\"unknown_outcomes\":" << m.unknown << ",\"timeouts\":" << m.timeouts
                << ",\"error_rate\":" << static_cast<double>(m.errors) / m.operations
                << ",\"timeout_rate\":" << static_cast<double>(m.timeouts) / m.operations
                << ",\"object_bytes\":" << m.object_bytes << ",\"chunks\":" << m.chunks << ",\"nodes\":" << m.nodes
                << ",\"replicas\":" << m.replicas << ",\"workers\":" << m.workers << ",\"elapsed_seconds\":" << m.wall
                << ",\"operations_per_second\":" << m.operations / m.wall << ",\"logical_bytes_per_second\":" << m.bytes / m.wall
                << ",\"logical_bytes\":" << m.bytes << ",\"p50_ms\":" << m.percentile(.50) << ",\"p95_ms\":" << m.percentile(.95)
                << ",\"p99_ms\":" << m.percentile(.99) << ",\"cpu_seconds\":";
            if (m.cpu < 0) out << "null"; else out << m.cpu;
            out << ",\"process_cpu_percent\":"; if (m.cpu < 0) out << "null"; else out << 100 * m.cpu / m.wall;
            out << ",\"disk_file_bytes\":" << m.disk << ",\"temporary_residual_bytes\":" << m.temporary
                << ",\"recovery_samples\":" << m.recovery_latencies.size();
            for (const auto p : {50, 95, 99}) {
                out << ",\"recovery_p" << p << "_ms\":";
                if (m.recovery_latencies.empty()) out << "null";
                else { auto values = m.recovery_latencies; std::sort(values.begin(), values.end());
                    out << values[static_cast<std::size_t>(std::ceil(static_cast<double>(p) / 100 * values.size())) - 1]; }
            }
            out << ",\"first_failure\":" << quote(m.failure) << '}';
        }
        return out.str() + "\n]}\n";
    }
};
class Partition final : public epoch::Participant {
public:
    explicit Partition(std::shared_ptr<replication::RemoteStorageNodeClient> client) : client(std::move(client)) {}
    std::string_view id() const noexcept override { return client->id(); }
    epoch::Result activate_epoch(const epoch::Token& previous, const epoch::Token& desired) override {
        return blocked ? epoch::Result{} : client->activate_epoch(previous, desired);
    }
    epoch::Result query_epoch() const override { return blocked ? epoch::Result{} : client->query_epoch(); }
    std::shared_ptr<replication::RemoteStorageNodeClient> client;
    bool blocked{};
};
void Harness::faults() {
    repair(false, repair::RepairExecutionFaultPoint::after_executing_persist);
    for (auto point : {repair::RepairExecutionFaultPoint::after_executing_persist,
                       repair::RepairExecutionFaultPoint::after_publication_persist,
                       repair::RepairExecutionFaultPoint::after_catalog_commit}) repair(true, point);
    {
        bench::Cluster cluster(1, config.chunk);
        const auto data = bench::payload(std::min<std::size_t>(config.bytes, config.chunk), config.seed);
        measure("lost_repair_response_restart_exact_replay", config.iterations, data.size(), 1, 1, cluster.root(), [&](auto n) {
            cluster.restart(0, config.chunk, true);
            const auto value = bench::manifest(data, config.chunk, "lost-gen-" + std::to_string(n));
            const auto id = "execution-" + std::to_string(n), object = "lost-" + std::to_string(n);
            bench::require(cluster.nodes[0].client->publish_repair("plan", id, "node-0", object, value, value.chunks[0], data).status ==
                replication::RepairPublishStatus::result_unknown, "lost response execution=" + id);
            const auto start = Clock::now(); cluster.restart(0, config.chunk);
            const auto replay = cluster.nodes[0].client->publish_repair("plan", id, "node-0", object, value, value.chunks[0], data);
            bench::require(replay.status == replication::RepairPublishStatus::published || replay.status == replication::RepairPublishStatus::already_present_valid,
                "exact replay execution=" + id);
            bench::require(cluster.nodes[0].client->get_chunk(object, value.generation_id, 0).data == data, "replay checksum");
            return Outcome{data.size(), false, true, false, std::chrono::duration<double, std::milli>(Clock::now() - start).count()};
        });
    }
    {
        bench::Cluster cluster(1, config.chunk);
        const auto data = bench::payload(std::min<std::size_t>(config.bytes, config.chunk), config.seed);
        measure("repair_transport_outage_exact_explicit_replay", config.iterations, data.size(), 1, 1, cluster.root(), [&](auto n) {
            const auto value = bench::manifest(data, config.chunk, "outage-gen-" + std::to_string(n));
            const auto id = "outage-execution-" + std::to_string(n), object = "outage-" + std::to_string(n);
            auto obsolete = cluster.nodes[0].client; cluster.stop(0);
            const auto failed = obsolete->publish_repair("plan", id, "node-0", object, value, value.chunks[0], data);
            bench::require(failed.status != replication::RepairPublishStatus::published &&
                failed.status != replication::RepairPublishStatus::already_present_valid, "unavailable node reported success");
            bench::require(!std::filesystem::exists(cluster.nodes[0].journal / "repairs" / id), "unexpected outage journal execution=" + id);
            cluster.start(0);
            bench::require(cluster.nodes[0].client->publish_repair("plan", id, "node-0", object, value, value.chunks[0], data).status ==
                replication::RepairPublishStatus::published, "explicit outage replay execution=" + id);
            return Outcome{data.size(), failed.status != replication::RepairPublishStatus::result_unknown,
                failed.status == replication::RepairPublishStatus::result_unknown,
                failed.status == replication::RepairPublishStatus::timeout};
        });
    }
    {
        bench::Cluster cluster(2, config.chunk);
        const auto data = bench::payload(config.bytes, config.seed);
        const auto plan = bench::placement("restart-object", bench::manifest(data, config.chunk, "restart-generation"), {"node-0", "node-1"});
        { replication::ReplicaManager manager(cluster.clients());
          bench::require(manager.put(plan, data).status == replication::ReplicationStatus::success, "restart seed"); }
        measure("replicated_node_restart_availability", config.iterations, config.bytes, 2, 2, cluster.root(), [&](auto) {
            cluster.stop(0);
            replication::ReplicaManager degraded({cluster.nodes[1].client});
            bench::require(degraded.get(plan).data == data, "temporary unavailability");
            cluster.nodes[0].local = std::make_shared<replication::LocalStorageNode>("node-0", cluster.nodes[0].data_root,
                object_protocol::kMaxObjectDataSize, config.chunk);
            cluster.start(0);
            replication::ReplicaManager recovered(cluster.clients());
            bench::require(recovered.get(plan).data == data, "restart availability");
            return Outcome{config.bytes};
        });
        auto partition = std::make_shared<Partition>(cluster.nodes[1].client);
        epoch::Coordinator activation(*cluster.authority, {cluster.nodes[0].client, partition});
        measure("partial_epoch_activation_explicit_resume_stale_rejection", config.iterations, 0, 2, 0, cluster.root(), [&](auto n) {
            partition->blocked = true;
            bench::require(activation.activate("coordinator", "next-" + std::to_string(n)).status == epoch::Status::unresolved, "partial epoch");
            bench::require(activation.reconcile().status == epoch::Status::unresolved, "partition reconciliation");
            bool blocked = false; try { cluster.authority->authorize(cluster.token); } catch (const epoch::Error&) { blocked = true; }
            bench::require(blocked, "pending epoch allowed old mutation");
            partition->blocked = false;
            bench::require(activation.resume_activation().status == epoch::Status::accepted, "explicit resume");
            bench::require(cluster.nodes[0].client->remove(plan.object_id, plan.generation_id) == storage::StoreError::stale_epoch,
                "stale epoch deletion accepted");
            return Outcome{0, true, true};
        });
    }
}
} // namespace

int main(int argc, char** argv) {
    try {
        Harness harness(parse(argc, argv));
        if (harness.config.suite == "all" || harness.config.suite == "local") harness.local();
        if (harness.config.suite == "all" || harness.config.suite == "distributed") harness.distributed();
        if (harness.config.suite == "all" || harness.config.suite == "fault") harness.faults();
        const auto json = harness.json();
        if (harness.config.output.empty()) std::cout << json;
        else { std::ofstream output(harness.config.output); output << json; output.close();
            if (!output) throw std::runtime_error("benchmark output write failed"); }
        return std::any_of(harness.results.begin(), harness.results.end(), [](const auto& m) { return m.errors != 0; }) ? 1 : 0;
    } catch (const std::exception& error) { std::cerr << "benchmark failed: " << error.what() << '\n'; return 2; }
}
