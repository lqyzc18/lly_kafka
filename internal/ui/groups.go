package ui

import (
	"fmt"

	"lly_kafka/internal/core"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

type groupsPanel struct {
	win        fyne.Window
	client     *core.KafkaClient
	loaded     bool
	refreshSeq uint64
	resetting  bool

	allGroups []core.GroupInfo
	groups    []core.GroupInfo
	search    *widget.Entry
	list      *widget.List
	selected  string

	infoHead *widget.Label
	table    *widget.Table
	rows     []core.GroupTopicOffset
}

func newGroupsPanel(win fyne.Window, client *core.KafkaClient) *groupsPanel {
	return &groupsPanel{win: win, client: client}
}

func (p *groupsPanel) build() fyne.CanvasObject {
	btn := widget.NewButtonWithIcon("刷新", theme.ViewRefreshIcon(), p.refresh)
	btnLatest := widget.NewButton("重置到最新", func() { p.onReset(core.OffsetResetLatest) })
	btnEarliest := widget.NewButton("重置到最早", func() { p.onReset(core.OffsetResetEarliest) })
	p.search = widget.NewEntry()
	p.search.SetPlaceHolder("搜索消费组...")
	p.search.OnChanged = func(string) {
		p.applyFilter()
		if p.list != nil {
			p.list.Refresh()
		}
	}
	head := container.NewVBox(container.NewHBox(btn, btnLatest, btnEarliest), p.search)

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
	p.infoHead.Wrapping = fyne.TextWrapWord
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
	split.Offset = 0.32
	return split
}

func (p *groupsPanel) applyFilter() {
	q := ""
	if p.search != nil {
		q = p.search.Text
	}
	if q == "" {
		p.groups = append([]core.GroupInfo(nil), p.allGroups...)
		return
	}
	names := make([]string, len(p.allGroups))
	for i, g := range p.allGroups {
		names[i] = g.Name
	}
	keep := filterTopics(names, q)
	set := map[string]struct{}{}
	for _, n := range keep {
		set[n] = struct{}{}
	}
	out := make([]core.GroupInfo, 0, len(keep))
	for _, g := range p.allGroups {
		if _, ok := set[g.Name]; ok {
			out = append(out, g)
		}
	}
	p.groups = out
	if p.selected != "" {
		found := false
		for _, group := range p.groups {
			if group.Name == p.selected {
				found = true
				break
			}
		}
		if !found {
			p.selected = ""
			if p.list != nil {
				p.list.UnselectAll()
			}
			if p.infoHead != nil {
				p.infoHead.SetText("请选择消费组")
			}
			p.rows = nil
			if p.table != nil {
				p.table.Refresh()
			}
		}
	}
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

func (p *groupsPanel) ensureLoaded() {
	if p.loaded {
		return
	}
	p.loaded = true
	p.refresh()
}

func (p *groupsPanel) refresh() {
	p.refreshSeq++
	seq := p.refreshSeq
	fetchAndShow(p.client.Context(), func() ([]core.GroupInfo, error) {
		return p.client.ListGroups(p.client.Context())
	}, func(groups []core.GroupInfo, err error) {
		if seq != p.refreshSeq {
			return
		}
		if err != nil {
			showError(p.win, "获取消费组失败", err)
			return
		}
		p.allGroups = groups
		p.applyFilter()
		if p.list != nil {
			p.list.Refresh()
		}
	})
}

func (p *groupsPanel) refreshOffsets() {
	group := p.selected
	if group == "" {
		return
	}
	fetchAndShow(p.client.Context(), func() ([]core.GroupTopicOffset, error) {
		return p.client.GroupOffsets(p.client.Context(), group)
	}, func(rows []core.GroupTopicOffset, err error) {
		if p.selected != group {
			return
		}
		if err != nil {
			p.infoHead.SetText(group + " (加载失败: " + err.Error() + ")")
			return
		}
		p.rows = rows
		total, n := core.GroupLagSummary(rows)
		p.infoHead.SetText(fmt.Sprintf("消费组: %s  ·  %d 个分区  ·  总 Lag %d  ·  %d 个分区落后", group, len(rows), total, n))
		p.table.Refresh()
	})
}

func (p *groupsPanel) onReset(to string) {
	if p.selected == "" {
		showInfo(p.win, "提示", "请先选择消费组")
		return
	}
	group := p.selected
	label := "最新"
	if to == core.OffsetResetEarliest {
		label = "最早"
	}
	dialog.ShowConfirm("重置 Offset",
		fmt.Sprintf("将消费组「%s」的全部已提交分区重置到%s offset。\n进行中的消费者可能报错并 rebalance，确定继续？", group, label),
		func(ok bool) {
			if !ok {
				return
			}
			if p.resetting {
				showInfo(p.win, "提示", "正在重置 Offset，请稍候")
				return
			}
			p.resetting = true
			go func() {
				err := p.client.ResetGroupOffsets(p.client.Context(), group, to)
				fyne.Do(func() {
					if p.client.Context().Err() != nil {
						return
					}
					p.resetting = false
					if err != nil {
						showError(p.win, "重置失败", err)
						return
					}
					showInfo(p.win, "成功", "已重置消费组: "+group)
					p.refreshOffsets()
				})
			}()
		}, p.win)
}
