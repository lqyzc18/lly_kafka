package ui

import (
	"fmt"
	"sort"
	"strings"

	"lly_kafka/internal/core"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/gogf/gf/v2/util/gconv"
)

// topicsPanel Topic 列表与详情 Tab。
// topicsPanel Topic 管理面板，负责列表、详情、新建、删除、扩分区和改配置。
type topicsPanel struct {
	win        fyne.Window
	client     *core.KafkaClient
	loaded     bool
	refreshSeq uint64

	search    *widget.Entry
	list      *widget.List
	topics    []string
	filtered  []string
	selected  string
	detailTtl *widget.Label
	infoLbl   *widget.Label
	partTable *widget.Table
	parts     []core.PartitionInfo
	// partsFor 记录 parts 所属的 Topic，避免详情尚未返回时把上一个 Topic
	// 的分区数用于删除确认或扩分区校验。
	partsFor string
	cfgLbl   *widget.Label
}

// newTopicsPanel 构造 Topic Tab。
// newTopicsPanel 创建 Topic 面板实例。
func newTopicsPanel(win fyne.Window, client *core.KafkaClient) *topicsPanel {
	return &topicsPanel{win: win, client: client, topics: []string{}, filtered: []string{}}
}

// build 构建 Topic 面板的 UI 组件和事件绑定。
func (p *topicsPanel) build() fyne.CanvasObject {
	// 顶部工具条
	btnRefresh := widget.NewButtonWithIcon("刷新列表", theme.ViewRefreshIcon(), p.refresh)
	btnCreate := widget.NewButtonWithIcon("新建 Topic", theme.ContentAddIcon(), p.onCreate)
	btnExpand := widget.NewButtonWithIcon("扩分区", theme.ContentAddIcon(), p.onExpand)
	btnAlter := widget.NewButtonWithIcon("改配置", theme.DocumentCreateIcon(), p.onAlterConfig)
	btnDelete := widget.NewButtonWithIcon("删除 Topic", theme.DeleteIcon(), p.onDelete)
	top := container.NewHBox(btnRefresh, btnCreate, btnExpand, btnAlter, btnDelete)

	// 左侧 Topic 列表 + 搜索
	p.search = widget.NewEntry()
	p.search.SetPlaceHolder("搜索 Topic...")
	p.search.OnChanged = func(string) {
		p.applyFilter()
		p.list.Refresh()
	}
	p.list = widget.NewList(
		func() int { return len(p.filtered) },
		func() fyne.CanvasObject {
			l := widget.NewLabel("")
			l.Truncation = fyne.TextTruncateEllipsis
			return l
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id >= len(p.filtered) {
				return
			}
			obj.(*widget.Label).SetText(p.filtered[id])
		},
	)
	p.list.OnSelected = func(id widget.ListItemID) {
		if id >= len(p.filtered) {
			return
		}
		p.onSelect(p.filtered[id])
	}

	left := container.NewBorder(p.search, nil, nil, nil, p.list)

	// 右侧详情
	p.detailTtl = widget.NewLabelWithStyle("请选择 Topic", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	p.infoLbl = widget.NewLabel("")
	p.infoLbl.Wrapping = fyne.TextWrapWord

	p.partTable = widget.NewTable(
		func() (int, int) { return len(p.parts) + 1, 7 },
		func() fyne.CanvasObject { return widget.NewLabel("") },
		p.updatePartCell,
	)
	p.partTable.SetColumnWidth(0, 70)
	p.partTable.SetColumnWidth(1, 70)
	p.partTable.SetColumnWidth(2, 200)
	p.partTable.SetColumnWidth(3, 200)
	p.partTable.SetColumnWidth(4, 100)
	p.partTable.SetColumnWidth(5, 100)
	p.partTable.SetColumnWidth(6, 100)

	btnRefreshDetail := widget.NewButtonWithIcon("刷新详情", theme.ViewRefreshIcon(), p.refreshDetail)
	rightTop := container.NewVBox(p.detailTtl, p.infoLbl, btnRefreshDetail)

	p.cfgLbl = widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{})
	p.cfgLbl.Wrapping = fyne.TextWrapWord
	cfgScroll := container.NewVScroll(p.cfgLbl)
	cfgScroll.SetMinSize(fyne.NewSize(0, 120))

	partScroll := container.NewVScroll(p.partTable)
	partScroll.SetMinSize(fyne.NewSize(0, 200))

	right := container.NewBorder(
		rightTop, nil, nil, nil,
		container.NewVSplit(partScroll, cfgScroll),
	)

	split := container.NewHSplit(left, right)
	split.Offset = 0.28

	return container.NewBorder(top, nil, nil, nil, split)
}

// updatePartCell 渲染分区详情表格单元格，处理越界行清空。
func (p *topicsPanel) updatePartCell(id widget.TableCellID, obj fyne.CanvasObject) {
	label := obj.(*widget.Label)
	label.Importance = widget.MediumImportance
	label.TextStyle = fyne.TextStyle{}
	if id.Row == 0 {
		label.TextStyle = fyne.TextStyle{Bold: true}
		switch id.Col {
		case 0:
			label.SetText("分区")
		case 1:
			label.SetText("Leader")
		case 2:
			label.SetText("Replicas")
		case 3:
			label.SetText("ISR")
		case 4:
			label.SetText("Start")
		case 5:
			label.SetText("End")
		case 6:
			label.SetText("消息数")
		}
		return
	}
	if id.Row-1 >= len(p.parts) {
		label.SetText("")
		return
	}
	part := p.parts[id.Row-1]
	under := part.UnderReplicated()
	switch id.Col {
	case 0:
		label.SetText(fmt.Sprintf("%d", part.ID))
	case 1:
		label.SetText(fmt.Sprintf("%d", part.Leader))
	case 2:
		label.SetText(formatInts(part.Replicas))
	case 3:
		if under {
			label.Importance = widget.DangerImportance
			label.SetText(formatInts(part.ISR) + " ⚠")
		} else {
			label.SetText(formatInts(part.ISR))
		}
	case 4:
		label.SetText(fmt.Sprintf("%d", part.StartOffset))
	case 5:
		label.SetText(fmt.Sprintf("%d", part.EndOffset))
	case 6:
		label.SetText(fmt.Sprintf("%d", part.MessageCount))
	}
}

// formatInts 将 int32 切片格式化为逗号分隔字符串。
func formatInts(xs []int32) string {
	ss := make([]string, 0, len(xs))
	for _, x := range xs {
		ss = append(ss, fmt.Sprintf("%d", x))
	}
	return strings.Join(ss, ",")
}

// applyFilter 根据搜索框过滤 Topic，并处理选中项不可见时的清理。
func (p *topicsPanel) applyFilter() {
	q := ""
	if p.search != nil {
		q = p.search.Text
	}
	p.filtered = filterTopics(p.topics, q)
	if p.selected != "" && !contains(p.filtered, p.selected) {
		p.selected = ""
		if p.list != nil {
			p.list.UnselectAll()
		}
		if p.detailTtl != nil {
			p.detailTtl.SetText("请选择 Topic")
		}
		if p.infoLbl != nil {
			p.infoLbl.SetText("")
		}
		p.parts = nil
		p.partsFor = ""
		if p.partTable != nil {
			p.partTable.Refresh()
		}
	}
}

// ensureLoaded 确保 Topic 列表已加载，仅首次调用时刷新。
func (p *topicsPanel) ensureLoaded() {
	if p.loaded {
		return
	}
	p.loaded = true
	p.refresh()
}

// refresh 强制刷新 Topic 列表。
func (p *topicsPanel) refresh() {
	p.refreshSeq++
	seq := p.refreshSeq
	fetchAndShow(p.client.Context(), func() ([]string, error) {
		return p.client.RefreshTopicNames(p.client.Context())
	}, func(names []string, err error) {
		if seq != p.refreshSeq {
			return
		}
		if err != nil {
			showError(p.win, "获取 Topic 列表失败", err)
			return
		}
		p.topics = names
		p.applyFilter()
		if p.list != nil {
			p.list.Refresh()
		}
	})
}

// onSelect 处理 Topic 选中事件，触发详情加载。
func (p *topicsPanel) onSelect(name string) {
	p.selected = name
	// 立即清空上一个 Topic 的详情，避免加载期间展示错位数据。
	p.parts = nil
	p.partsFor = ""
	if p.cfgLbl != nil {
		p.cfgLbl.SetText("")
	}
	if p.infoLbl != nil {
		p.infoLbl.SetText("加载中...")
	}
	if p.partTable != nil {
		p.partTable.Refresh()
	}
	p.refreshDetail()
}

// refreshDetail 拉取所选 Topic 详情。
func (p *topicsPanel) refreshDetail() {
	name := p.selected
	if name == "" {
		return
	}
	fetchAndShow(p.client.Context(), func() (*core.TopicInfo, error) {
		return p.client.TopicDetail(p.client.Context(), name)
	}, func(info *core.TopicInfo, err error) {
		if p.selected != name {
			return
		}
		if err != nil {
			p.detailTtl.SetText("加载失败: " + name)
			p.infoLbl.SetText(err.Error())
			p.parts = nil
			p.partsFor = ""
			p.partTable.Refresh()
			p.cfgLbl.SetText("")
			return
		}
		p.detailTtl.SetText(info.Name)
		under := 0
		var msgs int64
		for _, part := range info.Partitions {
			if part.UnderReplicated() {
				under++
			}
			if part.MessageCount > 0 {
				msgs += part.MessageCount
			}
		}
		extra := ""
		if under > 0 {
			extra = fmt.Sprintf("   ⚠ ISR 不足: %d 分区", under)
		}
		p.infoLbl.SetText(fmt.Sprintf("分区数: %d   副本因子: %d   约 %d 条消息%s", info.NumPartitions, info.ReplicationFactor, msgs, extra))
		p.parts = info.Partitions
		p.partsFor = name
		p.partTable.Refresh()

		keys := make([]string, 0, len(info.Configs))
		for k := range info.Configs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var cfgs []string
		for _, k := range keys {
			cfgs = append(cfgs, fmt.Sprintf("%s = %s", k, info.Configs[k]))
		}
		if len(cfgs) == 0 {
			p.cfgLbl.SetText("(无 Topic 配置)")
		} else {
			p.cfgLbl.SetText("Topic 配置：\n" + strings.Join(cfgs, "\n"))
		}
	})
}

// onCreate 弹出「新建 Topic」对话框。
func (p *topicsPanel) onCreate() {
	nameE := widget.NewEntry()
	nameE.SetPlaceHolder("topic-name")
	partsE := widget.NewEntry()
	partsE.SetText("1")
	rfE := widget.NewEntry()
	rfE.SetText("1")
	configsE := widget.NewEntry()
	configsE.SetPlaceHolder("k1=v1,k2=v2 (可选)")

	items := []*widget.FormItem{
		{Text: "名称", Widget: nameE},
		{Text: "分区数", Widget: partsE},
		{Text: "副本因子", Widget: rfE},
		{Text: "配置", Widget: configsE},
	}
	var d dialog.Dialog
	d = dialog.NewForm("新建 Topic", "创建", "取消", items, func(ok bool) {
		if !ok {
			return
		}
		name := strings.TrimSpace(nameE.Text)
		if name == "" {
			showInfo(p.win, "提示", "名称不能为空")
			d.Show()
			return
		}
		partsN := gconv.Int32(strings.TrimSpace(partsE.Text))
		rfN := gconv.Int16(strings.TrimSpace(rfE.Text))
		if partsN <= 0 {
			partsN = 1
		}
		if rfN <= 0 {
			rfN = 1
		}
		cfgs := parseKV(strings.TrimSpace(configsE.Text))
		go func() {
			err := p.client.CreateTopic(p.client.Context(), name, partsN, rfN, cfgs)
			fyne.Do(func() {
				if p.client.Context().Err() != nil {
					return
				}
				if err != nil {
					showError(p.win, "创建 Topic 失败", err)
					return
				}
				showInfo(p.win, "成功", "已创建 Topic: "+name)
				p.refresh()
			})
		}()
	}, p.win)
	d.Resize(fyne.NewSize(460, 340))
	d.Show()
}

// onDelete 删除当前选中的 Topic。
func (p *topicsPanel) onDelete() {
	if p.selected == "" {
		showInfo(p.win, "提示", "请先选择 Topic")
		return
	}
	name := p.selected
	// 详情尚未返回时不展示分区数，避免误用上一个 Topic 的数据。
	msg := fmt.Sprintf("确定删除 Topic「%s」吗？\n此操作不可恢复。", name)
	if p.partsFor == name && len(p.parts) > 0 {
		msg = fmt.Sprintf("确定删除 Topic「%s」吗？\n分区数: %d\n此操作不可恢复。", name, len(p.parts))
	}
	dialog.ShowConfirm("确认删除", msg, func(ok bool) {
		if !ok {
			return
		}
		go func() {
			err := p.client.DeleteTopic(p.client.Context(), name)
			fyne.Do(func() {
				if p.client.Context().Err() != nil {
					return
				}
				if err != nil {
					showError(p.win, "删除失败", err)
					return
				}
				showInfo(p.win, "成功", "已删除: "+name)
				p.selected = ""
				p.refresh()
			})
		}()
	}, p.win)
}

func (p *topicsPanel) onExpand() {
	if p.selected == "" {
		showInfo(p.win, "提示", "请先选择 Topic")
		return
	}
	cur := int32(1)
	if p.partsFor == p.selected && len(p.parts) > 0 {
		cur = int32(len(p.parts))
	}
	countE := widget.NewEntry()
	countE.SetText(fmt.Sprintf("%d", cur+1))
	items := []*widget.FormItem{
		{Text: "目标分区数", Widget: countE},
	}
	name := p.selected
	var d dialog.Dialog
	d = dialog.NewForm("扩容分区 - "+name, "扩容", "取消", items, func(ok bool) {
		if !ok {
			return
		}
		n := gconv.Int32(strings.TrimSpace(countE.Text))
		if n <= cur {
			showInfo(p.win, "提示", fmt.Sprintf("目标分区数必须大于当前 %d", cur))
			d.Show()
			return
		}
		go func() {
			err := p.client.ExpandPartitions(p.client.Context(), name, n)
			fyne.Do(func() {
				if p.client.Context().Err() != nil {
					return
				}
				if err != nil {
					showError(p.win, "扩容失败", err)
					return
				}
				showInfo(p.win, "成功", fmt.Sprintf("已将 %s 扩至 %d 分区", name, n))
				p.refreshDetail()
			})
		}()
	}, p.win)
	d.Resize(fyne.NewSize(400, 180))
	d.Show()
}

func (p *topicsPanel) onAlterConfig() {
	if p.selected == "" {
		showInfo(p.win, "提示", "请先选择 Topic")
		return
	}
	cfgE := widget.NewEntry()
	cfgE.SetPlaceHolder("retention.ms=86400000,cleanup.policy=delete")
	items := []*widget.FormItem{
		{Text: "配置", Widget: cfgE},
	}
	name := p.selected
	var d dialog.Dialog
	d = dialog.NewForm("修改 Topic 配置 - "+name, "提交", "取消", items, func(ok bool) {
		if !ok {
			return
		}
		cfgs := parseKV(cfgE.Text)
		if len(cfgs) == 0 {
			showInfo(p.win, "提示", "请填写 k=v 配置")
			d.Show()
			return
		}
		go func() {
			err := p.client.AlterTopicConfig(p.client.Context(), name, cfgs)
			fyne.Do(func() {
				if p.client.Context().Err() != nil {
					return
				}
				if err != nil {
					showError(p.win, "修改配置失败", err)
					return
				}
				showInfo(p.win, "成功", "已提交配置修改")
				p.refreshDetail()
			})
		}()
	}, p.win)
	d.Resize(fyne.NewSize(480, 200))
	d.Show()
}
