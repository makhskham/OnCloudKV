package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/makhskham/oncloudkv/proto/gen"
)

var (
	serverAddr  string
	consistency string
	ttl         int64
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	root := &cobra.Command{
		Use:   "oncloudkv-cli",
		Short: "OnCloudKV command-line client",
	}
	root.PersistentFlags().StringVarP(&serverAddr, "server", "s", "localhost:7002", "gRPC server address")
	root.PersistentFlags().StringVarP(&consistency, "consistency", "c", "eventual", "consistency mode: strong|eventual|read-your-writes|monotonic")

	root.AddCommand(putCmd(), getCmd(), deleteCmd(), scanCmd(), watchCmd(), statusCmd(), chaosCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func dial() (pb.KVServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewKVServiceClient(conn), conn, nil
}

func putCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Write a key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := client.Put(ctx, &pb.PutRequest{
				Key:        args[0],
				Value:      []byte(args[1]),
				TtlSeconds: ttl,
			})
			if err != nil {
				return err
			}
			fmt.Printf("OK  version=%d\n", resp.RaftIndex)
			return nil
		},
	}
	cmd.Flags().Int64VarP(&ttl, "ttl", "t", 0, "TTL in seconds (0 = no expiry)")
	return cmd
}

func getCmd() *cobra.Command {
	var sessionIndex, minVersion int64
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Read a value by key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", consistency)

			resp, err := client.Get(ctx, &pb.GetRequest{
				Key:          args[0],
				Consistency:  consistency,
				SessionIndex: sessionIndex,
				MinVersion:   minVersion,
			})
			if err != nil {
				return err
			}
			if !resp.Found {
				fmt.Println("(not found)")
				return nil
			}
			fmt.Printf("%s  [version=%d]\n", string(resp.Value), resp.Version)
			return nil
		},
	}
	cmd.Flags().Int64Var(&sessionIndex, "session-index", 0, "Raft index of your last write (for read-your-writes)")
	cmd.Flags().Int64Var(&minVersion, "min-version", 0, "Minimum version watermark (for monotonic)")
	return cmd
}

func deleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: args[0]})
			if err != nil {
				return err
			}
			fmt.Printf("OK  version=%d\n", resp.RaftIndex)
			return nil
		},
	}
}

func scanCmd() *cobra.Command {
	var limit int32
	cmd := &cobra.Command{
		Use:   "scan [prefix]",
		Short: "List keys matching a prefix",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()

			prefix := ""
			if len(args) > 0 {
				prefix = args[0]
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			resp, err := client.Scan(ctx, &pb.ScanRequest{Prefix: prefix, Limit: limit})
			if err != nil {
				return err
			}
			for _, item := range resp.Items {
				fmt.Printf("%-40s  %s  [v=%d]\n", item.Key, string(item.Value), item.Version)
			}
			fmt.Printf("(%d results)\n", len(resp.Items))
			return nil
		},
	}
	cmd.Flags().Int32VarP(&limit, "limit", "l", 100, "Maximum results")
	return cmd
}

func watchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "watch [prefix]",
		Short: "Stream key-change events in real time",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()

			prefix := ""
			if len(args) > 0 {
				prefix = args[0]
			}

			fmt.Printf("Watching prefix %q - press Ctrl+C to stop\n", prefix)
			stream, err := client.Watch(context.Background(), &pb.WatchRequest{Prefix: prefix})
			if err != nil {
				return err
			}
			for {
				evt, err := stream.Recv()
				if err != nil {
					return err
				}
				op := "PUT"
				if evt.Type == pb.WatchEvent_DELETE {
					op = "DEL"
				}
				fmt.Printf("[%s] %s = %s  (v=%d)\n", op, evt.Key, string(evt.Value), evt.Version)
			}
		},
	}
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := client.Status(ctx, &pb.StatusRequest{})
			if err != nil {
				return err
			}
			fmt.Printf("Node ID:       %s\n", resp.NodeId)
			fmt.Printf("State:         %s\n", resp.State)
			fmt.Printf("Leader:        %s\n", resp.Leader)
			fmt.Printf("Commit index:  %s\n", strconv.FormatInt(resp.CommitIndex, 10))
			fmt.Printf("Applied index: %s\n", strconv.FormatInt(resp.AppliedIndex, 10))
			fmt.Printf("Peers:         %v\n", resp.Peers)
			return nil
		},
	}
}

func chaosCmd() *cobra.Command {
	var partitionNodes string
	chaos := &cobra.Command{
		Use:   "chaos",
		Short: "Fault injection for resilience testing",
	}

	chaos.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show current cluster state before injecting faults",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, conn, err := dial()
			if err != nil {
				return err
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := client.Status(ctx, &pb.StatusRequest{})
			if err != nil {
				return err
			}
			fmt.Printf("Chaos target: node=%s state=%s leader=%s\n", resp.NodeId, resp.State, resp.Leader)
			return nil
		},
	})

	chaos.AddCommand(&cobra.Command{
		Use:   "partition",
		Short: "Simulate a network partition by printing iptables rules to apply",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("To simulate partition, run on the target node:\n")
			fmt.Printf("  iptables -I INPUT -s %s -j DROP\n", partitionNodes)
			fmt.Printf("  iptables -I OUTPUT -d %s -j DROP\n", partitionNodes)
			fmt.Printf("To heal:\n")
			fmt.Printf("  iptables -D INPUT -s %s -j DROP\n", partitionNodes)
			fmt.Printf("  iptables -D OUTPUT -d %s -j DROP\n", partitionNodes)
			log.Info().Str("nodes", partitionNodes).Msg("chaos: partition rules printed")
			return nil
		},
	})

	chaos.PersistentFlags().StringVar(&partitionNodes, "nodes", "", "Comma-separated node addresses to partition")
	return chaos
}
