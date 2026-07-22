package app

import (
	"context"
	"time"

	"github.com/powerset-co/wacli/internal/wa"
)

func (a *App) refreshContacts(ctx context.Context) error {
	if err := a.OpenWA(); err != nil {
		return err
	}
	contacts, err := a.wa.GetAllContacts(ctx)
	if err != nil {
		return err
	}
	for jid, info := range contacts {
		jid = canonicalJID(jid)
		_ = a.db.UpsertContact(
			jid.String(),
			jid.User,
			info.PushName,
			info.FullName,
			info.FirstName,
			info.BusinessName,
		)
	}
	return nil
}

func (a *App) refreshGroups(ctx context.Context) error {
	if err := a.OpenWA(); err != nil {
		return err
	}
	groups, err := a.wa.GetJoinedGroups(ctx)
	if err != nil {
		return err
	}
	now := nowUTC()
	joined := map[string]bool{}
	for _, g := range groups {
		if g == nil {
			continue
		}
		joined[g.JID.String()] = true
		_ = a.db.UpsertGroupWithHierarchy(g.JID.String(), g.GroupName.Name, g.OwnerJID.String(), g.GroupCreated, g.IsParent, g.LinkedParentJID.String())
		// Group metadata refresh does not include a last-message timestamp. Keep
		// chat activity derived from actual messages instead of manufacturing a
		// fresh timestamp every time groups are refreshed.
		_ = a.db.UpsertChat(g.JID.String(), "group", g.GroupName.Name, time.Time{})
	}
	if err := a.db.MarkGroupsMissingFrom(joined, now); err != nil {
		return err
	}
	return a.db.ReconcileChatLastMessageTSForKind("group")
}

func (a *App) refreshNewsletters(ctx context.Context) error {
	if err := a.OpenWA(); err != nil {
		return err
	}
	list, err := a.wa.GetSubscribedNewsletters(ctx)
	if err != nil {
		return err
	}
	for _, meta := range list {
		if meta == nil {
			continue
		}
		name := wa.NewsletterName(meta)
		if name == "" {
			name = meta.ID.String()
		}
		// Newsletter metadata refresh does not prove message activity either.
		_ = a.db.UpsertChat(meta.ID.String(), "newsletter", name, time.Time{})
	}
	return a.db.ReconcileChatLastMessageTSForKind("newsletter")
}
