package ui

import (
	"context"
	"fmt"
	"strconv"

	"lly_kafka/internal/core"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
)

const partAll = "全部"

type messagesPanel struct {
	win      fyne.Window
	client   *core.KafkaClient
	cfg      *core.AppConfig
	prefs    fyne.Preferences
	connName string
	loaded   bool

	topics   []string
	topicSel *widget.Select
	partSel  *widget.Select

	mode       *widget.RadioGroup
	countE     *widget.Entry
	startE     *widget.Entry
	endE       *widget.Entry
	keyFilter  *widget.Entry
	valFilter  *widget.Entry
	btnFetch   *widget.Button
	btnCancel  *widget.Button
	btnProduce *widget.Button
	statusLbl  *widget.Label

	table       *widget.Table
	allRows     []core.MessageInfo
	rows        []core.MessageInfo
	fetchCancel context.CancelFunc
	restorePart string
}

func newMessagesPanel(win fyne.Window, client *core.KafkaClient, cfg *core.AppConfig, prefs fyne.Preferences, connName string) *messagesPanel {
	return &messagesPanel{
		win: win, client: client, cfg: cfg, prefs: prefs, connName: connName,
		rows: []core.MessageInfo{}, allRows: []core.MessageInfo{},
	}
}

func (p *messagesPanel) prefKey(kind string) string {
	return "msg." + kind + "." + p.connName
}

func (p *messagesPanel) build() fyne.CanvasObject {
	p.topicSel = widget.NewSelect([]string{}, func(s string) {
		if p.prefs != nil && p.connName != "" && s != "" {
			p.prefs.SetString(p.prefKey("topic"), s)
		}
		p.refreshPartitions()
	})
	p.partSel = widget.NewSelect([]string{}, func(s string) {
		if p.prefs != nil && p.connName != "" && s != "" {
			p.prefs.SetString(p.prefKey("part"), s)
		}
	})

	p.countE = widget.NewEntry()
	p.countE.SetText(strconv.Itoa(p.cfg.DefaultMessageCount))
	p.startE = widget.NewEntry()
	p.startE.SetText("0")
	p.endE = widget.NewEntry()
	p.endE.SetText("0")

	p.mode = widget.NewRadioGroup([]string{"最新 N 条", "指定 Offset 范围"}, func(string) {
		p.updateModeFields()
	})
	p.mode.Horizontal = true
	p.mode.SetSelected("最新 N 条")

	p.keyFilter = widget.NewEntry()
	p.keyFilter.SetPlaceHolder("过滤 Key")
	p.keyFilter.OnChanged = func(string) { p.applyMsgFilter() }
	p.valFilter = widget.NewEntry()
	p.valFilter.SetPlaceHolder("过滤 Value")
	p.valFilter.OnChanged = func(string) { p.applyMsgFilter() }

	p.btnFetch = widget.NewButtonWithIcon("拉取消息", theme.SearchIcon(), p.onFetch)
	p.btnCancel = widget.NewButtonWithIcon("取消", theme.CancelIcon(), p.onCancelFetch)
	p.btnCancel.Disable()
	p.btnProduce = widget.NewButtonWithIcon("发送消息", theme.MailSendIcon(), p.onProduce)
	p.statusLbl = widget.NewLabel("就绪")
	p.statusLbl.Importance = widget.MediumImportance

	p.table = widget.NewTable(
		func() (int, int) { return len(p.rows) + 1, 6 },
		func() fyne.CanvasObject {
			l := widget.NewLabel("")
			l.Truncation = fyne.TextTruncateEllipsis
			return l
		},
		p.updateCell,
	)
	p.table.SetColumnWidth(0, 100)
	p.table.SetColumnWidth(1, 80)
	p.table.SetColumnWidth(2, 180)
	p.table.SetColumnWidth(3, 400)
	p.table.SetColumnWidth(4, 200)
	p.table.SetColumnWidth(5, 80)
	p.table.OnSelected = func(id widget.TableCellID) {
		if id.Row == 0 || id.Row-1 >= len(p.rows) {
			return
		}
		p.showMessageDetail(p.rows[id.Row-1])
	}

	form := container.NewVBox(
		container.NewGridWithColumns(2,
			widget.NewLabel("Topic:"), p.topicSel,
			widget.NewLabel("分区:"), p.partSel,
		),
		container.NewHBox(widget.NewLabel("模式:"), p.mode),
		container.NewGridWithColumns(6,
			widget.NewLabel("N:"), p.countE,
			widget.NewLabel("起始:"), p.startE,
			widget.NewLabel("结束:"), p.endE,
		),
		container.NewGridWithColumns(4,
			widget.NewLabel("Key 含:"), p.keyFilter,
			widget.NewLabel("Value 含:"), p.valFilter,
		),
		container.NewHBox(p.btnFetch, p.btnCancel, p.btnProduce, p.statusLbl),
	)

	scroll := container.NewVScroll(p.table)
	scroll.SetMinSize(fyne.NewSize(600, 400))
	return container.NewBorder(form, nil, nil, nil, scroll)
}

func (p *messagesPanel) updateCell(id widget.TableCellID, obj fyne.CanvasObject) {
	label := obj.(*widget.Label)
	label.Importance = widget.MediumImportance
	label.TextStyle = fyne.TextStyle{}
	if id.Row == 0 {
		label.TextStyle = fyne.TextStyle{Bold: true}
		switch id.Col {
		case 0:
			label.SetText("Offset")
		case 1:
			label.SetText("分区")
		case 2:
			label.SetText("Key")
		case 3:
			label.SetText("Value")
		case 4:
			label.SetText("时间")
		case 5:
			label.SetText("大小")
		}
		return
	}
	if id.Row-1 >= len(p.rows) {
		return
	}
	m := p.rows[id.Row-1]
	switch id.Col {
	case 0:
		label.SetText(fmt.Sprintf("%d", m.Offset))
	case 1:
		label.SetText(fmt.Sprintf("%d", m.Partition))
	case 2:
		label.SetText(previewBytes(m.Key, 64))
	case 3:
		label.SetText(previewBytes(m.Value, p.cfg.MessagePreviewLen))
	case 4:
		if !m.Timestamp.IsZero() {
			label.SetText(gtime.New(m.Timestamp).Format("Y-m-d H:i:s"))
		} else {
			label.SetText("-")
		}
	case 5:
		label.SetText(fmt.Sprintf("%d", m.Size))
	}
}

func (p *messagesPanel) updateModeFields() {
	if p.countE == nil || p.startE == nil || p.endE == nil {
		return
	}
	if p.mode != nil && p.mode.Selected == "最新 N 条" {
		p.countE.Enable()
		p.startE.Disable()
		p.endE.Disable()
	} else {
		p.countE.Disable()
		p.startE.Enable()
		p.endE.Enable()
	}
}

func (p *messagesPanel) ensureLoaded() {
	if p.loaded {
		return
	}
	p.loaded = true
	p.refresh()
}

func (p *messagesPanel) refresh() {
	fetchAndShow(p.client.Context(), func() ([]string, error) {
		return p.client.ListTopicNames(p.client.Context())
	}, func(names []string, err error) {
		if err != nil {
			showError(p.win, "获取 Topic 失败", err)
			return
		}
		p.topics = names
		p.topicSel.Options = names
		if len(names) == 0 {
			p.topicSel.SetSelected("")
			p.partSel.Options = []string{}
			p.partSel.SetSelected("")
			return
		}
		want := ""
		if p.prefs != nil {
			want = p.prefs.String(p.prefKey("topic"))
		}
		if want == "" || !contains(names, want) {
			want = names[0]
		}
		if p.prefs != nil {
			p.restorePart = p.prefs.String(p.prefKey("part"))
		}
		p.topicSel.SetSelected(want)
	})
}

func (p *messagesPanel) refreshPartitions() {
	name := p.topicSel.Selected
	if name == "" {
		return
	}
	fetchAndShow(p.client.Context(), func() (int, error) {
		return p.client.PartitionCount(p.client.Context(), name)
	}, func(n int, err error) {
		if err != nil {
			showError(p.win, "获取分区数失败", err)
			return
		}
		if p.topicSel.Selected != name {
			return
		}
		opts := make([]string, 0, n+1)
		opts = append(opts, partAll)
		for i := 0; i < n; i++ {
			opts = append(opts, strconv.Itoa(i))
		}
		p.partSel.Options = opts
		want := p.restorePart
		p.restorePart = ""
		if want == "" || !contains(opts, want) {
			want = partAll
		}
		p.partSel.SetSelected(want)
	})
}

func (p *messagesPanel) applyMsgFilter() {
	kq, vq := "", ""
	if p.keyFilter != nil {
		kq = p.keyFilter.Text
	}
	if p.valFilter != nil {
		vq = p.valFilter.Text
	}
	p.rows = core.FilterMessages(p.allRows, kq, vq)
	if p.table != nil {
		p.table.Refresh()
	}
	if p.statusLbl != nil && len(p.allRows) > 0 {
		p.statusLbl.SetText(fmt.Sprintf("显示 %d / %d 条", len(p.rows), len(p.allRows)))
	}
}

func (p *messagesPanel) onCancelFetch() {
	if p.fetchCancel != nil {
		p.fetchCancel()
	}
}

func (p *messagesPanel) onFetch() {
	topic := p.topicSel.Selected
	partStr := p.partSel.Selected
	if topic == "" || partStr == "" {
		showInfo(p.win, "提示", "请选择 Topic 和分区")
		return
	}
	allParts := partStr == partAll
	part := int32(gconv.Int(partStr))
	latest := p.mode.Selected == "最新 N 条"
	n := int64(gconv.Int(p.countE.Text))
	if n <= 0 {
		n = int64(p.cfg.DefaultMessageCount)
	}
	start := gconv.Int64(p.startE.Text)
	end := gconv.Int64(p.endE.Text)
	if !latest && end > 0 && end < start {
		showInfo(p.win, "提示", "结束 offset 应大于或等于起始 offset")
		return
	}

	if p.fetchCancel != nil {
		p.fetchCancel()
	}
	fetchCtx, cancel := context.WithCancel(p.client.Context())
	p.fetchCancel = cancel

	p.btnFetch.Disable()
	p.btnCancel.Enable()
	p.statusLbl.SetText("拉取中...")
	max := p.cfg.FetchMaxMessages
	alive := p.client.Context()
	fetchAndShow(alive, func() ([]core.MessageInfo, error) {
		if allParts {
			if latest {
				return p.client.FetchLatestAll(fetchCtx, topic, int(n), max)
			}
			return p.client.FetchMessagesAll(fetchCtx, topic, start, end, max)
		}
		if latest {
			return p.client.FetchLatest(fetchCtx, topic, part, int(n), max)
		}
		return p.client.FetchMessages(fetchCtx, topic, part, start, end, max)
	}, func(msgs []core.MessageInfo, err error) {
		p.btnFetch.Enable()
		p.btnCancel.Disable()
		if err != nil {
			p.statusLbl.SetText("拉取失败")
			showError(p.win, "拉取消息失败", err)
			return
		}
		p.allRows = msgs
		p.applyMsgFilter()
		if len(p.allRows) == len(p.rows) {
			p.statusLbl.SetText(fmt.Sprintf("共 %d 条消息", len(msgs)))
		}
	})
}

func (p *messagesPanel) showMessageDetail(m core.MessageInfo) {
	keyE := widget.NewMultiLineEntry()
	keyE.SetText(core.FormatPayload(m.Key))
	keyE.Wrapping = fyne.TextWrapWord
	valE := widget.NewMultiLineEntry()
	valE.SetText(core.FormatPayload(m.Value))
	valE.Wrapping = fyne.TextWrapWord

	var hdrStr string
	for _, h := range m.Headers {
		hdrStr += fmt.Sprintf("%s = %s\n", h.Key, h.Value)
	}
	if hdrStr == "" {
		hdrStr = "(无 Headers)"
	}
	hdrLbl := widget.NewLabelWithStyle("Headers:\n"+hdrStr, fyne.TextAlignLeading, fyne.TextStyle{})
	hdrLbl.Wrapping = fyne.TextWrapWord

	content := container.NewBorder(
		widget.NewLabelWithStyle(fmt.Sprintf("Topic: %s   分区: %d   Offset: %d   大小: %d",
			m.Topic, m.Partition, m.Offset, m.Size), fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		hdrLbl, nil, nil,
		container.NewVSplit(
			container.NewBorder(widget.NewLabel("Key:"), nil, nil, nil, container.NewScroll(keyE)),
			container.NewBorder(widget.NewLabel("Value（JSON 自动缩进 / 二进制 hex）:"), nil, nil, nil, container.NewScroll(valE)),
		),
	)
	dlg := dialog.NewCustom("消息详情", "关闭", content, p.win)
	dlg.Resize(fyne.NewSize(760, 560))
	dlg.Show()
}

func (p *messagesPanel) onProduce() {
	if len(p.topics) == 0 {
		showInfo(p.win, "提示", "请先刷新 Topic 列表")
		return
	}
	topicSel := widget.NewSelect(p.topics, nil)
	topicSel.SetSelected(p.topicSel.Selected)
	partE := widget.NewEntry()
	partE.SetText("-1")
	keyE := widget.NewEntry()
	keyE.SetPlaceHolder("可选")
	valE := widget.NewMultiLineEntry()
	valE.SetPlaceHolder("消息内容")
	valE.Wrapping = fyne.TextWrapWord
	headersE := widget.NewEntry()
	headersE.SetPlaceHolder("k1=v1,k2=v2 (可选)")

	items := []*widget.FormItem{
		{Text: "Topic", Widget: topicSel},
		{Text: "分区 (留空或-1为自动)", Widget: partE},
		{Text: "Key", Widget: keyE},
		{Text: "Value", Widget: valE},
		{Text: "Headers", Widget: headersE},
	}
	d := dialog.NewForm("发送消息", "发送", "取消", items, func(ok bool) {
		if !ok {
			return
		}
		topic := topicSel.Selected
		if topic == "" {
			showInfo(p.win, "提示", "请选择 Topic")
			return
		}
		hdrs := parseKV(headersE.Text)
		var key []byte
		if s := keyE.Text; s != "" {
			key = []byte(s)
		}
		go func() {
			pid, offset, err := p.client.ProduceMessage(p.client.Context(), topic, int32(gconv.Int(partE.Text)),
				key, []byte(valE.Text), hdrs)
			fyne.Do(func() {
				if p.client.Context().Err() != nil {
					return
				}
				if err != nil {
					showError(p.win, "发送失败", err)
					return
				}
				showInfo(p.win, "成功", fmt.Sprintf("已发送至 %s 分区=%d offset=%d", topic, pid, offset))
			})
		}()
	}, p.win)
	d.Resize(fyne.NewSize(520, 420))
	d.Show()
}
