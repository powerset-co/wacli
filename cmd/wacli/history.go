package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/powerset-co/wacli/internal/app"
	"github.com/powerset-co/wacli/internal/out"
	"github.com/powerset-co/wacli/internal/store"
	"github.com/spf13/cobra"
)

func newHistoryCmd(flags *rootFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history",
		Short: "History coverage and backfill",
	}
	cmd.AddCommand(newHistoryCoverageCmd(flags))
	cmd.AddCommand(newHistoryFillCmd(flags))
	cmd.AddCommand(newHistoryBackfillCmd(flags))
	cmd.AddCommand(newHistoryBackfillBatchCmd(flags))
	return cmd
}

func newHistoryCoverageCmd(flags *rootFlags) *cobra.Command {
	var chats []string
	var query string
	var kind string
	var limit int
	var includeBlocked bool
	var onlyActionable bool

	cmd := &cobra.Command{
		Use:   "coverage",
		Short: "Show local archive coverage by chat",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := withTimeout(cmd.Context(), flags)
			defer cancel()

			a, lk, err := newApp(ctx, flags, false, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			coverage, err := a.DB().ListHistoryCoverage(store.ListHistoryCoverageParams{
				ChatJIDs:       chats,
				Query:          query,
				Kind:           kind,
				Limit:          limit,
				IncludeBlocked: includeBlocked,
				OnlyActionable: onlyActionable,
			})
			if err != nil {
				return err
			}
			if flags.asJSON {
				return out.WriteJSON(os.Stdout, map[string]any{"coverage": coverage})
			}
			return writeHistoryCoverageTable(os.Stdout, coverage, fullTableOutput(flags.fullOutput), false)
		},
	}
	cmd.Flags().StringSliceVar(&chats, "chat", nil, "chat JID to inspect (repeatable)")
	cmd.Flags().StringVar(&query, "query", "", "filter chats by local name or JID")
	cmd.Flags().StringVar(&kind, "kind", "", "chat kind filter (dm|group|broadcast|newsletter|unknown)")
	cmd.Flags().IntVar(&limit, "limit", 100, "limit rows")
	cmd.Flags().BoolVar(&includeBlocked, "include-blocked", false, "include chats without a local message anchor")
	cmd.Flags().BoolVar(&onlyActionable, "only-actionable", false, "show only chats with a local message anchor")
	return cmd
}

func newHistoryFillCmd(flags *rootFlags) *cobra.Command {
	var chats []string
	var query string
	var kind string
	var limit int
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "fill",
		Short: "Plan multi-chat history backfill",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !dryRun {
				return fmt.Errorf("history fill currently supports --dry-run only; use history backfill --chat JID to request history")
			}

			ctx, cancel := withTimeout(cmd.Context(), flags)
			defer cancel()

			a, lk, err := newApp(ctx, flags, false, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			coverage, err := a.DB().ListHistoryCoverage(store.ListHistoryCoverageParams{
				ChatJIDs:       chats,
				Query:          query,
				Kind:           kind,
				Limit:          limit,
				IncludeBlocked: true,
			})
			if err != nil {
				return err
			}
			selected := historyFillCandidates(coverage)
			if flags.asJSON {
				return out.WriteJSON(os.Stdout, map[string]any{
					"selected": selected,
					"coverage": coverage,
				})
			}

			fmt.Fprintf(os.Stdout, "Selected %d chats for fill dry run.\n", len(selected))
			return writeHistoryCoverageTable(os.Stdout, coverage, fullTableOutput(flags.fullOutput), true)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show which chats would be selected without connecting")
	cmd.Flags().StringSliceVar(&chats, "chat", nil, "chat JID to consider (repeatable)")
	cmd.Flags().StringVar(&query, "query", "", "filter chats by local name or JID")
	cmd.Flags().StringVar(&kind, "kind", "", "chat kind filter (dm|group|broadcast|newsletter|unknown)")
	cmd.Flags().IntVar(&limit, "limit", 100, "limit rows")
	return cmd
}

func newHistoryBackfillCmd(flags *rootFlags) *cobra.Command {
	var chat string
	var count int
	var requests int
	var wait time.Duration
	var requestDelay time.Duration
	var idleExit time.Duration

	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Request older messages for a chat from your primary device (on-demand history sync)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if chat == "" {
				return fmt.Errorf("--chat is required")
			}
			if err := flags.requireWritable(); err != nil {
				return err
			}

			ctx, stop := signalContextWithEvents(out.NewEventWriter(os.Stderr, flags.events))
			defer stop()

			a, lk, err := newApp(ctx, flags, true, false)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			res, err := a.BackfillHistory(ctx, app.BackfillOptions{
				ChatJID:        chat,
				Count:          count,
				Requests:       requests,
				WaitPerRequest: wait,
				RequestDelay:   requestDelay,
				IdleExit:       idleExit,
			})
			if err != nil {
				return err
			}

			if flags.asJSON {
				return out.WriteJSON(os.Stdout, map[string]any{
					"chat":                 res.ChatJID,
					"requests_sent":        res.RequestsSent,
					"responses_seen":       res.ResponsesSeen,
					"messages_received":    res.MessagesReceived,
					"messages_added":       res.MessagesAdded,
					"messages_synced":      res.MessagesSynced,
					"other_messages_added": res.OtherMessagesAdded,
				})
			}

			fmt.Fprintf(
				os.Stdout,
				"Backfill complete for %s. Added %d target messages; %d other messages arrived while connected (%d requests).\n",
				res.ChatJID,
				res.MessagesAdded,
				res.OtherMessagesAdded,
				res.RequestsSent,
			)
			return nil
		},
	}

	cmd.Flags().StringVar(&chat, "chat", "", "chat JID")
	cmd.Flags().IntVar(&count, "count", app.DefaultBackfillCount, "number of messages to request per on-demand sync")
	cmd.Flags().IntVar(&requests, "requests", app.DefaultBackfillRequests, "number of on-demand requests to attempt")
	cmd.Flags().DurationVar(&wait, "wait", 60*time.Second, "time to wait for an on-demand response per request")
	cmd.Flags().DurationVar(&requestDelay, "request-delay", 0, "pause between on-demand history requests")
	cmd.Flags().DurationVar(&idleExit, "idle-exit", 5*time.Second, "exit after being idle (after backfill requests)")
	return cmd
}

func newHistoryBackfillBatchCmd(flags *rootFlags) *cobra.Command {
	var chats []string
	var count int
	var batchSize int
	var maxInFlight int
	var lidFallback bool
	var requests int
	var requestDelay time.Duration
	var wait time.Duration
	var batchDelay time.Duration
	var timeoutBackoff time.Duration
	var idleExit time.Duration

	cmd := &cobra.Command{
		Use:   "backfill-batch",
		Short: "Request older messages for multiple chats over one connection",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(chats) == 0 {
				return fmt.Errorf("at least one --chat is required")
			}
			if err := flags.requireWritable(); err != nil {
				return err
			}

			ctx, stop := signalContextWithEvents(out.NewEventWriter(os.Stderr, flags.events))
			defer stop()

			a, lk, err := newApp(ctx, flags, true, false)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			res, err := a.BackfillHistoryBatch(ctx, app.BackfillBatchOptions{
				ChatJIDs:       chats,
				Count:          count,
				BatchSize:      batchSize,
				MaxInFlight:    maxInFlight,
				LIDFallback:    lidFallback,
				Requests:       requests,
				RequestDelay:   requestDelay,
				WaitPerBatch:   wait,
				BatchDelay:     batchDelay,
				TimeoutBackoff: timeoutBackoff,
				IdleExit:       idleExit,
			})
			if err != nil {
				return err
			}

			chatResults := make([]map[string]any, 0, len(res.Chats))
			responded := 0
			timedOut := 0
			failed := 0
			for _, chat := range res.Chats {
				switch {
				case chat.Error == "" && chat.ResponsesSeen > 0:
					responded++
				case strings.Contains(strings.ToLower(chat.Error), "timed out"):
					timedOut++
				default:
					failed++
				}
				chatResults = append(chatResults, map[string]any{
					"chat":              chat.ChatJID,
					"requests_sent":     chat.RequestsSent,
					"responses_seen":    chat.ResponsesSeen,
					"messages_received": chat.MessagesReceived,
					"messages_added":    chat.MessagesAdded,
					"request_identity":  chat.RequestIdentity,
					"end_type":          chat.EndType,
					"elapsed_ms":        chat.Elapsed.Milliseconds(),
					"error":             chat.Error,
				})
			}
			if flags.asJSON {
				status := "completed"
				if responded != len(res.Chats) {
					status = "partial"
				}
				return out.WriteJSON(os.Stdout, map[string]any{
					"status": status,
					"chats":  chatResults,
					"counts": map[string]any{
						"total":     len(res.Chats),
						"responded": responded,
						"timed_out": timedOut,
						"failed":    failed,
					},
					"messages_synced":      res.MessagesSynced,
					"other_messages_added": res.OtherMessagesAdded,
					"elapsed_ms":           res.Elapsed.Milliseconds(),
				})
			}

			fmt.Fprintf(
				os.Stdout,
				"Batch backfill finished for %d chats in %s: %d responded, %d timed out, %d failed; %d other messages arrived while connected.\n",
				len(res.Chats),
				res.Elapsed.Round(time.Millisecond),
				responded,
				timedOut,
				failed,
				res.OtherMessagesAdded,
			)
			for _, chat := range res.Chats {
				status := "ok"
				if chat.Error != "" {
					status = chat.Error
				}
				fmt.Fprintf(
					os.Stdout,
					"%s: requests=%d responses=%d received=%d added=%d identity=%s elapsed=%s status=%s\n",
					chat.ChatJID,
					chat.RequestsSent,
					chat.ResponsesSeen,
					chat.MessagesReceived,
					chat.MessagesAdded,
					chat.RequestIdentity,
					chat.Elapsed.Round(time.Millisecond),
					status,
				)
			}
			return nil
		},
	}

	cmd.Flags().StringSliceVar(&chats, "chat", nil, "chat JID to backfill (repeatable)")
	cmd.Flags().IntVar(&count, "count", app.DefaultBackfillCount, "number of messages to request per on-demand sync")
	cmd.Flags().IntVar(&batchSize, "batch-size", app.DefaultBackfillBatchSize, "number of chats to process before a batch delay")
	cmd.Flags().IntVar(&maxInFlight, "max-inflight", 10, "maximum simultaneous outstanding history requests")
	cmd.Flags().BoolVar(&lidFallback, "lid-fallback", true, "try the alternate mapped PN/LID identity after a timeout or empty first response")
	cmd.Flags().IntVar(&requests, "requests", 10, "maximum accepted history requests per chat")
	cmd.Flags().DurationVar(&requestDelay, "request-delay", 10*time.Second, "pause before requesting another chunk for chats that grew")
	cmd.Flags().DurationVar(&wait, "wait", 10*time.Second, "response window per in-flight request wave")
	cmd.Flags().DurationVar(&batchDelay, "batch-delay", 10*time.Second, "pause between batches")
	cmd.Flags().DurationVar(&timeoutBackoff, "timeout-backoff", time.Minute, "pause after a batch receives no protocol responses")
	cmd.Flags().DurationVar(&idleExit, "idle-exit", 5*time.Second, "exit after being idle after all batches")
	return cmd
}

func historyFillCandidates(coverage []store.HistoryCoverage) []store.HistoryCoverage {
	out := make([]store.HistoryCoverage, 0, len(coverage))
	for _, c := range coverage {
		if c.Status == store.HistoryCoverageStatusReady {
			out = append(out, c)
		}
	}
	return out
}

func writeHistoryCoverageTable(dst io.Writer, coverage []store.HistoryCoverage, fullOutput, includeSelected bool) error {
	w := newTableWriter(dst)
	if includeSelected {
		fmt.Fprintln(w, "SELECTED\tCHAT\tKIND\tMESSAGES\tOLDEST\tNEWEST\tSTATUS\tDETAIL")
	} else {
		fmt.Fprintln(w, "CHAT\tKIND\tMESSAGES\tOLDEST\tNEWEST\tSTATUS\tDETAIL")
	}
	for _, c := range coverage {
		name := c.Name
		if strings.TrimSpace(name) == "" {
			name = c.ChatJID
		}
		detail := historyCoverageDetail(c)
		selected := ""
		if includeSelected {
			if c.Status == store.HistoryCoverageStatusReady {
				selected = "yes"
			} else {
				selected = "no"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
				selected,
				tableCell(name, 32, fullOutput),
				c.Kind,
				c.MessageCount,
				formatHistoryDate(c.OldestTS),
				formatHistoryDate(c.NewestTS),
				c.Status,
				tableCell(detail, 36, fullOutput),
			)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			tableCell(name, 32, fullOutput),
			c.Kind,
			c.MessageCount,
			formatHistoryDate(c.OldestTS),
			formatHistoryDate(c.NewestTS),
			c.Status,
			tableCell(detail, 36, fullOutput),
		)
	}
	_ = w.Flush()
	return nil
}

func historyCoverageDetail(c store.HistoryCoverage) string {
	if c.BlockedReason != "" {
		return c.BlockedReason
	}
	return c.ChatJID
}

func formatHistoryDate(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02")
}
