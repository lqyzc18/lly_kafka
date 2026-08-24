package ui

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"lly_kafka/internal/core"
)

// groupsPanel 消费组 Tab。
type groupsPanel struct {
	win    fyne.Window
	client *core.KafkaClient

	groups  []core.GroupInfo
	list    *widget.List
	selected string

	infoHead *widget.Label
	table    *widget.Table
	rows     []core.GroupTopicOffset
}

func newGroupsPanel(win fyne.Window, client *core.KafkaClient) *groupsPanel {
	return &groupsPanel{win: win, client: client}
}

func (p *groupsPanel) build() fyne.CanvasObject {
	btn := widget.NewButtonWithIcon("刷新", theme.ViewRefreshIcon(), p.refresh)
	head := container.NewHBox(btn)

	p.list = widget.NewList(
		func() int { return len(p.groups) },
		func() fyne.CanvasObject {
			l := widget.NewLabel("")
			l.Truncation = fyne.TextTruncateEllipsis
			return l
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id >= len(p.groups) {
				return
			}
			g := p.groups[id]
			obj.(*widget.Label).SetText(fmt.Sprintf("%s (%s · %d members)", g.Name, g.State, g.Members))
		},
	)
	p.list.OnSelected = func(id widget.ListItemID) {
		if id >= len(p.groups) {
			return
		}
		p.selected = p.groups[id].Name
		p.refreshOffsets()
	}

	left := container.NewBorder(head, nil, nil, nil, p.list)

	p.infoHead = widget.NewLabelWithStyle("请选择消费组", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	p.table = widget.NewTable(
		func() (int, int) { return len(p.rows) + 1, 5 },
		func() fyne.CanvasObject {
			l := widget.NewLabel("")
			l.Truncation = fyne.TextTruncateEllipsis
			return l
		},
		p.updateCell,
	)
	p.table.SetColumnWidth(0, 200)
	p.table.SetColumnWidth(1, 80)
	p.table.SetColumnWidth(2, 140)
	p.table.SetColumnWidth(3, 140)
	p.table.SetColumnWidth(4, 120)

	right := container.NewBorder(p.infoHead, nil, nil, nil,
		container.NewVScroll(p.table))

	split := container.NewHSplit(left, right)
	split.Offset = 0.28
	return split
}

func (p *groupsPanel) updateCell(id widget.TableCellID, obj fyne.CanvasObject) {
	label := obj.(*widget.Label)
	label.Importance = widget.MediumImportance
	label.TextStyle = fyne.TextStyle{}
	if id.Row == 0 {
		label.TextStyle = fyne.TextStyle{Bold: true}
		switch id.Col {
		case 0:
			label.SetText("Topic")
		case 1:
			label.SetText("分区")
		case 2:
			label.SetText("消费 Offset")
		case 3:
			label.SetText("最新 Offset")
		case 4:
			label.SetText("Lag")
		}
		return
	}
	if id.Row-1 >= len(p.rows) {
		return
	}
	r := p.rows[id.Row-1]
	switch id.Col {
	case 0:
		label.SetText(r.Topic)
	case 1:
		label.SetText(fmt.Sprintf("%d", r.Partition))
	case 2:
		label.SetText(fmt.Sprintf("%d", r.GroupOffset))
	case 3:
		label.SetText(fmt.Sprintf("%d", r.LogEndOffset))
	case 4:
		if r.Lag > 0 {
			label.Importance = widget.DangerImportance
			label.SetText(fmt.Sprintf("⚠ %d", r.Lag))
		} else {
			label.SetText(fmt.Sprintf("%d", r.Lag))
		}
	}
}

// refresh 拉取消费组列表。
func (p *groupsPanel) refresh() {
	go func() {
		groups, err := p.client.ListGroups(core.Ctx)
		fyne.Do(func() {
			if err != nil {
				showError(p.win, "获取消费组失败", err)
				return
			}
			p.groups = groups
			p.list.Refresh()
		})
	}()
}

// refreshOffsets 拉取所选消费组的 offset 与 lag。
func (p *groupsPanel) refreshOffsets() {
	if p.selected == "" {
		return
	}
	group := p.selected
	go func() {
		rows, err := p.client.GroupOffsets(core.Ctx, group)
		fyne.Do(func() {
			if err != nil {
				p.infoHead.SetText(group + " (加载失败: " + err.Error() + ")")
				return
			}
			p.infoHead.SetText(fmt.Sprintf("消费组: %s  ·  %d 个分区", group, len(rows)))
			p.rows = rows
			p.table.Refresh()
		})
	}()
}