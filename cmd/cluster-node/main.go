// Command cluster-node runs a single Worker, Aggregator or Coordinator actor
// as its own process, reachable over gRPC. Running several instances (on
// different ports, machines or Docker containers) demonstrates the FL
// pipeline actually distributed across a cluster rather than one process.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/fl"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote/remotepb"
)

func main() {
	role := flag.String("role", "", "coordinator | worker | aggregator | provider")
	nodeID := flag.String("node-id", "", "this node's actor ID")
	listen := flag.String("listen", ":9000", "host:port this node's actor-transport gRPC server listens on")
	advertiseAddr := flag.String("advertise-addr", "", "host:port other nodes should dial to reach this one (defaults to -listen; must be set explicitly when -listen is a wildcard like 0.0.0.0:9000, e.g. to the container's own service name)")

	aggregatorAddr := flag.String("aggregator-addr", "", "host:port of the Aggregator (worker, coordinator)")
	aggregatorID := flag.String("aggregator-id", "aggregator", "actor ID of the Aggregator")

	coordinatorAddr := flag.String("coordinator-addr", "", "host:port of the Coordinator (aggregator)")
	coordinatorID := flag.String("coordinator-id", "coordinator", "actor ID of the Coordinator")

	workersFlag := flag.String("workers", "", "coordinator only: comma-separated id@host:port list of Workers")
	dataPath := flag.String("data", "data/SMSSpamCollection", "coordinator only: path to the SMS Spam Collection TSV file")
	partitionsDir := flag.String("partitions-dir", "data/partitions", "coordinator only: shared directory to write per-worker training partitions (must be readable by every worker)")
	trainRatio := flag.Float64("train-ratio", 0.8, "coordinator only: fraction of the dataset used for training")
	seed := flag.Int64("seed", 42, "coordinator only: random seed for the train/test split")
	roundTimeout := flag.Duration("round-timeout", 60*time.Second, "coordinator only: how long to wait for the FL round to complete")

	clusterMode := flag.String("cluster-mode", "none", "none | provider | p2p — join a cluster layer alongside the FL role")
	providerAddr := flag.String("provider-addr", "", "cluster-mode=provider: host:port of the ProviderActor")
	providerID := flag.String("provider-id", "provider", "cluster-mode=provider: actor ID of the ProviderActor")
	p2pBind := flag.String("p2p-bind", "0.0.0.0:7946", "cluster-mode=p2p: host:port memberlist gossips on")
	p2pSeeds := flag.String("p2p-seeds", "", "cluster-mode=p2p: comma-separated memberlist bind addrs to join through (empty for the first node)")
	flag.Parse()

	if *role == "" || *nodeID == "" {
		fmt.Fprintln(os.Stderr, "usage: cluster-node -role=coordinator|worker|aggregator|provider -node-id=<id> ...")
		os.Exit(2)
	}

	system := actor.NewActorSystem()
	grpcServer, lis, err := startTransport(system, *listen)
	if err != nil {
		log.Fatalf("cluster-node: start transport on %s: %v", *listen, err)
	}
	defer grpcServer.GracefulStop()
	log.Printf("cluster-node: role=%s node-id=%s listening on %s", *role, *nodeID, lis.Addr())

	selfAddr := *advertiseAddr
	if selfAddr == "" {
		selfAddr = *listen
	}

	if *role != "provider" {
		joinCluster(system, *nodeID, selfAddr, *clusterMode, *providerAddr, *providerID, *p2pBind, *p2pSeeds)
	}

	switch *role {
	case "worker":
		runWorker(system, *nodeID, *aggregatorAddr, *aggregatorID)
	case "aggregator":
		runAggregator(system, *nodeID, *coordinatorAddr, *coordinatorID)
	case "coordinator":
		runCoordinator(system, *nodeID, *aggregatorAddr, *aggregatorID, *workersFlag, *dataPath, *partitionsDir, *trainRatio, *seed, *roundTimeout)
	case "provider":
		runProvider(system, *nodeID)
	default:
		log.Fatalf("cluster-node: unknown -role=%q", *role)
	}
}

// joinCluster optionally spawns a cluster-layer actor alongside the node's FL
// role, in the same ActorSystem and (for provider mode) reusing the same
// gRPC transport server already listening on selfAddr.
func joinCluster(system *actor.ActorSystem, nodeID, selfAddr, mode, providerAddr, providerID, p2pBind, p2pSeeds string) {
	clusterID := nodeID + "-cluster"
	switch mode {
	case "none":
		return
	case "provider":
		if providerAddr == "" {
			log.Fatal("cluster-node: -cluster-mode=provider requires -provider-addr")
		}
		client := cluster.NewProviderClientActor(clusterID, selfAddr, providerID, providerAddr)
		if _, err := system.Spawn(client); err != nil {
			log.Fatalf("cluster-node: spawn provider client: %v", err)
		}
		log.Printf("cluster-node: %s joined cluster via provider at %s", nodeID, providerAddr)
	case "p2p":
		var seeds []string
		if p2pSeeds != "" {
			seeds = strings.Split(p2pSeeds, ",")
		}
		p2p := cluster.NewP2PActor(clusterID, p2pBind, selfAddr, seeds)
		if _, err := system.Spawn(p2p); err != nil {
			log.Fatalf("cluster-node: spawn p2p actor: %v", err)
		}
		log.Printf("cluster-node: %s joined cluster via gossip, bound on %s", nodeID, p2pBind)
	default:
		log.Fatalf("cluster-node: unknown -cluster-mode=%q", mode)
	}
}

// runProvider spawns a ProviderActor and blocks forever, waiting for
// JoinCluster/DiscoverPeers requests from other nodes over gRPC.
func runProvider(system *actor.ActorSystem, nodeID string) {
	if _, err := system.Spawn(cluster.NewProviderActor(nodeID)); err != nil {
		log.Fatalf("cluster-node: spawn provider: %v", err)
	}
	log.Printf("cluster-node: provider %s ready", nodeID)
	select {} // serve forever
}

func startTransport(system *actor.ActorSystem, listen string) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, nil, err
	}
	grpcServer := grpc.NewServer()
	remotepb.RegisterTransportServer(grpcServer, remote.NewServer(system))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("cluster-node: gRPC server stopped: %v", err)
		}
	}()
	return grpcServer, lis, nil
}

// runWorker spawns a WorkerActor and blocks forever, waiting for
// TrainRequest messages routed to it over gRPC.
func runWorker(system *actor.ActorSystem, nodeID, aggregatorAddr, aggregatorID string) {
	if aggregatorAddr == "" {
		log.Fatal("cluster-node: worker requires -aggregator-addr")
	}
	aggregatorRef, conn, err := remote.NewRef(aggregatorAddr, aggregatorID)
	if err != nil {
		log.Fatalf("cluster-node: dial aggregator: %v", err)
	}
	defer conn.Close()

	_, err = system.Spawn(&fl.WorkerActor{WorkerID: nodeID, Aggregator: aggregatorRef})
	if err != nil {
		log.Fatalf("cluster-node: spawn worker: %v", err)
	}

	log.Printf("cluster-node: worker %s ready, aggregator at %s", nodeID, aggregatorAddr)
	select {} // serve forever
}

// runAggregator spawns an AggregatorActor and blocks forever, waiting for
// LocalModelUpdate messages from workers and forwarding the FedAvg result
// to the Coordinator.
func runAggregator(system *actor.ActorSystem, nodeID, coordinatorAddr, coordinatorID string) {
	if coordinatorAddr == "" {
		log.Fatal("cluster-node: aggregator requires -coordinator-addr")
	}
	coordinatorRef, conn, err := remote.NewRef(coordinatorAddr, coordinatorID)
	if err != nil {
		log.Fatalf("cluster-node: dial coordinator: %v", err)
	}
	defer conn.Close()

	_, err = system.Spawn(fl.NewAggregatorActor(nodeID, coordinatorRef))
	if err != nil {
		log.Fatalf("cluster-node: spawn aggregator: %v", err)
	}

	log.Printf("cluster-node: aggregator %s ready, coordinator at %s", nodeID, coordinatorAddr)
	select {} // serve forever
}

// runCoordinator loads the dataset, writes per-worker partitions to a
// shared directory, runs one FL round across the real network, evaluates
// the result and exits.
func runCoordinator(system *actor.ActorSystem, nodeID, aggregatorAddr, aggregatorID, workersFlag, dataPath, partitionsDir string, trainRatio float64, seed int64, roundTimeout time.Duration) {
	if aggregatorAddr == "" {
		log.Fatal("cluster-node: coordinator requires -aggregator-addr")
	}
	workers, err := parseWorkers(workersFlag)
	if err != nil {
		log.Fatalf("cluster-node: -workers: %v", err)
	}
	if len(workers) == 0 {
		log.Fatal("cluster-node: coordinator requires -workers=id@host:port[,id@host:port...]")
	}

	records, err := dataset.Load(dataPath)
	if err != nil {
		log.Fatalf("cluster-node: load dataset: %v", err)
	}
	train, test := dataset.SplitTrainTest(records, trainRatio, seed)
	log.Printf("cluster-node: coordinator %s: %d train / %d test messages", nodeID, len(train), len(test))

	baselineModel := nb.Aggregate([]nb.LocalCounts{nb.CountLocal(train)})
	baseline := nb.Evaluate(baselineModel, test)
	log.Printf("cluster-node: coordinator %s: centralized baseline accuracy=%.4f", nodeID, baseline.Accuracy)

	partitionPaths, err := writePartitions(train, len(workers), partitionsDir)
	if err != nil {
		log.Fatalf("cluster-node: write partitions: %v", err)
	}

	aggregatorRef, aggConn, err := remote.NewRef(aggregatorAddr, aggregatorID)
	if err != nil {
		log.Fatalf("cluster-node: dial aggregator: %v", err)
	}
	defer aggConn.Close()

	workerRefs := make([]actor.Ref, len(workers))
	var workerConns []io.Closer
	for i, w := range workers {
		ref, conn, err := remote.NewRef(w.addr, w.id)
		if err != nil {
			log.Fatalf("cluster-node: dial worker %s: %v", w.id, err)
		}
		workerRefs[i] = ref
		workerConns = append(workerConns, conn)
	}
	defer func() {
		for _, c := range workerConns {
			c.Close()
		}
	}()

	results := make(chan fl.RoundResult, 1)
	coordinator := &fl.CoordinatorActor{
		CoordinatorID: nodeID,
		Workers:       workerRefs,
		Aggregator:    aggregatorRef,
		TestSet:       test,
		Results:       results,
	}
	if _, err := system.Spawn(coordinator); err != nil {
		log.Fatalf("cluster-node: spawn coordinator: %v", err)
	}

	coordinatorPID, _ := system.Lookup(nodeID)
	if err := coordinatorPID.Tell(fl.StartTraining{PartitionPaths: partitionPaths}); err != nil {
		log.Fatalf("cluster-node: start training: %v", err)
	}

	select {
	case result := <-results:
		printResult(result.Metrics, baseline)
	case <-time.After(roundTimeout):
		log.Fatal("cluster-node: timed out waiting for FL round to complete")
	}
}

const (
	ansiReset = "\033[0m"
	ansiBold  = "\033[1m"
	ansiCyan  = "\033[36m"
	ansiGreen = "\033[32m"
	ansiRed   = "\033[31m"
)

// printResult prints the round outcome as a banner that stands out in a
// terminal streaming interleaved logs from several containers at once
// (docker compose up shows all services' output live, line by line).
func printResult(federated, baseline nb.Metrics) {
	relative := federated.Accuracy / baseline.Accuracy
	status, color := "FAIL", ansiRed
	if relative >= 0.95 {
		status, color = "PASS", ansiGreen
	}

	line := strings.Repeat("=", 62)
	fmt.Println()
	fmt.Println(ansiBold + ansiCyan + line + ansiReset)
	fmt.Println(ansiBold + ansiCyan + "  REZULTAT FEDERATIVNE RUNDE" + ansiReset)
	fmt.Println(ansiBold + ansiCyan + line + ansiReset)
	fmt.Printf("  %-24s accuracy=%.4f  precision=%.4f  recall=%.4f  f1=%.4f\n",
		"Federativni model:", federated.Accuracy, federated.Precision, federated.Recall, federated.F1)
	fmt.Printf("  %-24s accuracy=%.4f  precision=%.4f  recall=%.4f  f1=%.4f\n",
		"Centralni baseline:", baseline.Accuracy, baseline.Precision, baseline.Recall, baseline.F1)
	fmt.Printf("  Odnos federativni/baseline: %.4f  (cilj >= 0.95)\n", relative)
	fmt.Println()
	fmt.Printf("  %s%s>>> %s <<<%s\n", ansiBold, color, status, ansiReset)
	fmt.Println(ansiBold + ansiCyan + line + ansiReset)
	fmt.Println()
}

type workerAddr struct {
	id   string
	addr string
}

// parseWorkers parses "id@host:port,id2@host:port2" into workerAddr entries.
func parseWorkers(spec string) ([]workerAddr, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	entries := strings.Split(spec, ",")
	workers := make([]workerAddr, 0, len(entries))
	for _, e := range entries {
		parts := strings.SplitN(strings.TrimSpace(e), "@", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid worker entry %q, want id@host:port", e)
		}
		workers = append(workers, workerAddr{id: parts[0], addr: parts[1]})
	}
	return workers, nil
}

// writePartitions splits train into n shards and saves each as its own TSV
// file under dir, which must be a path every worker can also read (e.g. a
// shared Docker volume mounted at the same location in every container).
func writePartitions(train []dataset.Record, n int, dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create partitions dir: %w", err)
	}

	shards := dataset.Partition(train, n)
	paths := make([]string, n)
	for i, shard := range shards {
		path := filepath.Join(dir, fmt.Sprintf("worker-%d.tsv", i))
		if err := dataset.Save(shard, path); err != nil {
			return nil, fmt.Errorf("save partition %d: %w", i, err)
		}
		paths[i] = path
	}
	return paths, nil
}
