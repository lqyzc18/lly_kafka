package ui

import (
	"fmt"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/text/gstr"
	"lly_kafka/internal/core"
)

// topicsPanel Topic 列表与详情 Tab。
type topicsPanel struct {
	win    fyne.Window
	client *core.KafkaClient
	cfg    *core.AppConfig

	search    *widget.Entry
	list      *widget.List
	topics    []string
	filtered  []string
	selected  string
	detailTtl *widget.Label
	infoLbl   *widget.Label
	partTable *widget.Table
	parts     []core.PartitionInfo
	cfgLbl    *widget.Label
}

// newTopicsPanel 构造 Topic Tab。
func newTopicsPanel(win fyne.Window, client *core.KafkaClient, cfg *core.AppConfig) *topicsPanel {
	return &topicsPanel{win: win, client: client, cfg: cfg, topics: []string{}, filtered: []string{}}
}

func (p *topicsPanel) build() fyne.CanvasObject {
	// 顶部工具条
	btnRefresh := widget.NewButtonWithIcon("刷新列表", theme.ViewRefreshIcon(), p.refresh)
	btnCreate := widget.NewButtonWithIcon("新建 Topic", theme.ContentAddIcon(), p.onCreate)
	btnDelete := widget.NewButtonWithIcon("删除 Topic", theme.DeleteIcon(), p.onDelete)
	top := container.NewHBox(btnRefresh, btnCreate, btnDelete)

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
		return
	}
	part := p.parts[id.Row-1]
	switch id.Col {
	case 0:
		label.SetText(fmt.Sprintf("%d", part.ID))
	case 1:
		label.SetText(fmt.Sprintf("%d", part.Leader))
	case 2:
		label.SetText(formatInts(part.Replicas))
	case 3:
		label.SetText(formatInts(part.ISR))
	case 4:
		label.SetText(fmt.Sprintf("%d", part.StartOffset))
	case 5:
		label.SetText(fmt.Sprintf("%d", part.EndOffset))
	case 6:
		label.SetText(fmt.Sprintf("%d", part.MessageCount))
	}
}

func formatInts(xs []int32) string {
	ss := make([]string, 0, len(xs))
	for _, x := range xs {
		ss = append(ss, fmt.Sprintf("%d", x))
	}
	return strings.Join(ss, ",")
}

// applyFilter 根据搜索框过滤 Topic。
func (p *topicsPanel) applyFilter() {
	q := strings.ToLower(strings.TrimSpace(p.search.Text))
	if q == "" {
		p.filtered = p.topics
		return
	}
	p.filtered = p.filtered[:0]
	for _, t := range p.topics {
		if gstr.ContainsI(t, q) {
			p.filtered = append(p.filtered, t)
		}
	}
}

// refresh 拉取 Topic 列表（异步）。
func (p *topicsPanel) refresh() {
	go func() {
		names, err := p.client.ListTopicNames(core.Ctx)
		fyne.Do(func() {
			if err != nil {
				showError(p.win, "获取 Topic 列表失败", err)
				return
			}
			p.topics = names
			p.applyFilter()
			p.list.Refresh()
		})
	}()
}

// onSelect 选择 Topic 后加载详情。
func (p *topicsPanel) onSelect(name string) {
	p.selected = name
	p.refreshDetail()
}

// refreshDetail 拉取所选 Topic 详情。
func (p *topicsPanel) refreshDetail() {
	if p.selected == "" {
		return
	}
	go func() {
		info, err := p.client.TopicDetail(core.Ctx, p.selected)
		fyne.Do(func() {
			if err != nil {
				p.detailTtl.SetText("加载失败: " + p.selected)
				p.infoLbl.SetText(err.Error())
				return
			}
			p.detailTtl.SetText(info.Name)
			p.infoLbl.SetText(fmt.Sprintf("分区数: %d   副本因子: %d", info.NumPartitions, info.ReplicationFactor))
			p.parts = info.Partitions
			p.partTable.Refresh()

			var cfgs []string
			for k, v := range info.Configs {
				cfgs = append(cfgs, fmt.Sprintf("%s = %s", k, v))
			}
			if len(cfgs) == 0 {
				p.cfgLbl.SetText("(无 Topic 配置)")
			} else {
				p.cfgLbl.SetText("Topic 配置：\n" + strings.Join(cfgs, "\n"))
			}
		})
	}()
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
	remarkE := widget.NewEntry()
	remarkE.SetPlaceHolder("备注 (可选，未使用)")

	items := []*widget.FormItem{
		{Text: "名称", Widget: nameE},
		{Text: "分区数", Widget: partsE},
		{Text: "副本因子", Widget: rfE},
		{Text: "配置", Widget: configsE},
		{Text: "备注", Widget: remarkE},
	}
	d := dialog.NewForm("新建 Topic", "创建", "取消", items, func(ok bool) {
		if !ok {
			return
		}
		name := strings.TrimSpace(nameE.Text)
		if name == "" {
			showInfo(p.win, "提示", "名称不能为空")
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
		var cfgs map[string]string
		if raw := strings.TrimSpace(configsE.Text); raw != "" {
			cfgs = map[string]string{}
			for _, kv := range strings.Split(raw, ",") {
				if idx := strings.Index(kv, "="); idx > 0 {
					cfgs[strings.TrimSpace(kv[:idx])] = strings.TrimSpace(kv[idx+1:])
				}
			}
		}
		go func() {
			err := p.client.CreateTopic(core.Ctx, name, partsN, rfN, cfgs)
			fyne.Do(func() {
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
	dialog.ShowConfirm("确认删除", "确定要删除 Topic 「"+name+"」吗？", func(ok bool) {
		if !ok {
			return
		}
		go func() {
			err := p.client.DeleteTopic(core.Ctx, name)
			fyne.Do(func() {
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