package app

import (
	"context"

	"go.mau.fi/whatsmeow/types/events"
)

// Link pairs this device and returns at its first login, before any history is
// read. That login uploads the device's prekeys, so the phone can send it the
// account's history; WhatsApp holds that history for the next sync.
func (a *App) Link(ctx context.Context, opts SyncOptions) error {
	if err := a.OpenWA(); err != nil {
		return err
	}
	loggedIn := make(chan struct{}, 1)
	handlerID := a.wa.AddEventHandler(func(evt interface{}) {
		if _, ok := evt.(*events.Connected); ok {
			select {
			case loggedIn <- struct{}{}:
			default:
			}
		}
	})
	defer a.wa.RemoveEventHandler(handlerID)
	if err := a.connectForSync(ctx, opts); err != nil {
		return err
	}
	select {
	case <-loggedIn:
	case <-ctx.Done():
		return ctx.Err()
	}
	a.emitOrPrint("linked", nil, "\nLinked.\n")
	return nil
}
