package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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

// messagesPanel 消息浏览面板，负责 Topic/分区选择、拉取、过滤、详情、发送和导出。
type messagesPanel struct {
	win      fyne.Window
	client   *core.KafkaClient
	cfg      *core.AppConfig
	prefs    fyne.Preferences
	connName string
	loaded   bool

	topics   []string
	topicSel *filterSelect
	partSel  *widget.Select

	mode       *widget.RadioGroup
	countE     *widget.Entry
	startE     *widget.Entry
	endE       *widget.Entry
	timeE      *widget.Entry
	keyFilter  *widget.Entry
	valFilter  *widget.Entry
	btnFetch   *widget.Button
	btnCancel  *widget.Button
	btnProduce *widget.Button
	btnExport  *widget.Button
	statusLbl  *widget.Label

	table        *widget.Table
	allRows      []core.MessageInfo
	rows         []core.MessageInfo
	fetchCancel  context.CancelFunc
	fetchSeq     uint64
	restorePart  string
	partUpdating bool
}

// newMessagesPanel 创建消息面板实例。
func newMessagesPanel(win fyne.Window, client *core.KafkaClient, cfg *core.AppConfig, prefs fyne.Preferences, connName string) *messagesPanel {
	return &messagesPanel{
		win: win, client: client, cfg: cfg, prefs: prefs, connName: connName,
		rows: []core.MessageInfo{}, allRows: []core.MessageInfo{},
	}
}

// prefKey 生成带连接名的偏好键，用于记住 Topic/分区选择。
func (p *messagesPanel) prefKey(kind string) string {
	return "msg." + kind + "." + p.connName
}

// build 构建消息面板的 UI 组件和事件绑定。
func (p *messagesPanel) build() fyne.CanvasObject {
	p.topicSel = newFilterSelect("筛选 Topic", func(s string) {
		if p.prefs != nil && p.connName != "" && s != "" {
			p.prefs.SetString(p.prefKey("topic"), s)
		}
		p.refreshPartitions()
	})
	p.partSel = widget.NewSelect([]string{}, func(s string) {
		if p.prefs != nil && p.connName != "" && s != "" {
			p.prefs.SetString(p.prefKey("part"), s)
		}
		if !p.partUpdating && s != "" {
			p.fetch(true)
		}
	})

	p.countE = widget.NewEntry()
	p.countE.SetText(strconv.Itoa(p.cfg.DefaultMessageCount))
	p.startE = widget.NewEntry()
	p.startE.SetText("0")
	p.endE = widget.NewEntry()
	p.endE.SetText("0")

	p.mode = widget.NewRadioGroup([]string{"最新 N 条", "指定 Offset 范围", "按时间"}, func(string) {
		p.updateModeFields()
	})
	p.mode.Horizontal = true
	p.mode.SetSelected("最新 N 条")

	p.timeE = widget.NewEntry()
	p.timeE.SetPlaceHolder("2026-08-26 12:00:00")

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
	p.btnExport = widget.NewButtonWithIcon("导出 JSON", theme.DocumentSaveIcon(), p.onExport)
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
		p.topicSel.filterWidget(),
		container.NewGridWithColumns(2,
			widget.NewLabel("Topic:"), p.topicSel.selectWidget(),
			widget.NewLabel("分区:"), p.partSel,
		),
		container.NewHBox(widget.NewLabel("模式:"), p.mode),
		container.NewGridWithColumns(6,
			widget.NewLabel("N:"), p.countE,
			widget.NewLabel("起始:"), p.startE,
			widget.NewLabel("结束:"), p.endE,
		),
		container.NewGridWithColumns(2,
			widget.NewLabel("起始时间:"), p.timeE,
		),
		container.NewGridWithColumns(4,
			widget.NewLabel("Key 含:"), p.keyFilter,
			widget.NewLabel("Value 含:"), p.valFilter,
		),
		container.NewHBox(p.btnFetch, p.btnCancel, p.btnProduce, p.btnExport, p.statusLbl),
	)

	scroll := container.NewVScroll(p.table)
	scroll.SetMinSize(fyne.NewSize(600, 400))
	p.updateModeFields()
	return container.NewBorder(form, nil, nil, nil, scroll)
}

// updateCell 渲染表格单元格，处理越界行清空。
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
		label.SetText("")
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

// updateModeFields 根据拉取模式启用/禁用输入框。
func (p *messagesPanel) updateModeFields() {
	if p.countE == nil || p.startE == nil || p.endE == nil || p.timeE == nil {
		return
	}
	mode := ""
	if p.mode != nil {
		mode = p.mode.Selected
	}
	switch mode {
	case "最新 N 条":
		p.countE.Enable()
		p.startE.Disable()
		p.endE.Disable()
		p.timeE.Disable()
	case "按时间":
		p.countE.Disable()
		p.startE.Disable()
		p.endE.Disable()
		p.timeE.Enable()
	default: // 指定 Offset 范围
		p.countE.Disable()
		p.startE.Enable()
		p.endE.Enable()
		p.timeE.Disable()
	}
}

// ensureLoaded 标记面板已加载并刷新 Topic 列表。
func (p *messagesPanel) ensureLoaded() {
	p.loaded = true
	p.refresh()
}

// refresh 强制刷新 Topic 列表并恢复上次选择。
func (p *messagesPanel) refresh() {
	fetchAndShow(p.client.Context(), func() ([]string, error) {
		return p.client.RefreshTopicNames(p.client.Context())
	}, func(names []string, err error) {
		if err != nil {
			showError(p.win, "获取 Topic 失败", err)
			return
		}
		p.topics = names
		p.topicSel.SetOptions(names)
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

// refreshPartitions 加载所选 Topic 的分区列表，并恢复上次选择的分区。
func (p *messagesPanel) refreshPartitions() {
	name := p.topicSel.Selected()
	if name == "" {
		return
	}
	if p.btnFetch != nil {
		p.btnFetch.Disable()
	}
	if p.statusLbl != nil {
		p.statusLbl.SetText("加载分区...")
	}
	fetchAndShow(p.client.Context(), func() (int, error) {
		return p.client.PartitionCount(p.client.Context(), name)
	}, func(n int, err error) {
		if err != nil {
			if p.topicSel.Selected() != name {
				return
			}
			if p.btnFetch != nil {
				p.btnFetch.Enable()
			}
			if p.statusLbl != nil {
				p.statusLbl.SetText("加载分区失败")
			}
			showError(p.win, "获取分区数失败", err)
			return
		}
		if p.topicSel.Selected() != name {
			return
		}
		opts := make([]string, 0, n+1)
		opts = append(opts, partAll)
		for i := 0; i < n; i++ {
			opts = append(opts, strconv.Itoa(i))
		}
		p.partUpdating = true
		p.partSel.Options = opts
		want := p.restorePart
		p.restorePart = ""
		if want == "" || !contains(opts, want) {
			want = partAll
		}
		p.partSel.SetSelected(want)
		p.partUpdating = false
		if p.btnFetch != nil {
			p.btnFetch.Enable()
		}
		p.fetch(true)
	})
}

// applyMsgFilter 按 Key/Value 子串过滤消息列表。
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

// onCancelFetch 取消当前消息拉取。
func (p *messagesPanel) onCancelFetch() {
	if p.fetchCancel != nil {
		p.statusLbl.SetText("正在取消...")
		p.btnCancel.Disable()
		p.fetchCancel()
	}
}

// onFetch 处理「拉取消息」按钮点击，手动触发拉取。
func (p *messagesPanel) onFetch() {
	p.fetch(false)
}

// fetch 执行消息拉取。auto=true 表示自动触发（如选中 Topic 后），不弹错误提示。
func (p *messagesPanel) fetch(auto bool) {
	topic := p.topicSel.Selected()
	partStr := p.partSel.Selected
	if topic == "" || partStr == "" {
		if !auto {
			showInfo(p.win, "提示", "请选择 Topic 和分区")
		}
		return
	}
	allParts := partStr == partAll
	part := int32(gconv.Int(partStr))
	mode := p.mode.Selected
	latest := mode == "最新 N 条"
	byTime := mode == "按时间"
	n := int64(gconv.Int(p.countE.Text))
	if n <= 0 {
		n = int64(p.cfg.DefaultMessageCount)
	}
	start := gconv.Int64(p.startE.Text)
	end := gconv.Int64(p.endE.Text)
	if !latest && !byTime && end > 0 && end < start {
		if !auto {
			showInfo(p.win, "提示", "结束 offset 应大于或等于起始 offset")
		}
		return
	}
	var startTime time.Time
	if byTime {
		ts := strings.TrimSpace(p.timeE.Text)
		if ts == "" {
			if !auto {
				showInfo(p.win, "提示", "请输入起始时间")
			}
			return
		}
		t, err := parseTimeInput(ts)
		if err != nil {
			if !auto {
				showInfo(p.win, "提示", "时间格式无效，请使用 2026-08-26 12:00:00 或 2026-08-26")
			}
			return
		}
		startTime = t
	}

	if p.fetchCancel != nil {
		p.fetchCancel()
	}
	p.fetchSeq++
	seq := p.fetchSeq
	fetchCtx, cancel := context.WithCancel(p.client.Context())
	p.fetchCancel = cancel

	p.btnFetch.Disable()
	p.btnCancel.Enable()
	p.statusLbl.SetText("拉取中...")
	max := p.cfg.FetchMaxMessages
	alive := p.client.Context()
	fetchAndShow(alive, func() ([]core.MessageInfo, error) {
		if byTime {
			if allParts {
				return p.client.FetchByTimeAll(fetchCtx, topic, startTime, max)
			}
			return p.client.FetchByTime(fetchCtx, topic, part, startTime, max)
		}
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
		if seq != p.fetchSeq {
			return
		}
		p.btnFetch.Enable()
		p.btnCancel.Disable()
		p.fetchCancel = nil
		cancelled := errors.Is(err, context.Canceled)
		timedOut := errors.Is(err, context.DeadlineExceeded)
		if len(msgs) > 0 {
			p.allRows = msgs
			p.applyMsgFilter()
		}
		if cancelled {
			p.statusLbl.SetText(fmt.Sprintf("已取消，收到 %d 条消息", len(msgs)))
			return
		}
		if timedOut {
			p.statusLbl.SetText(fmt.Sprintf("超时，收到 %d 条消息", len(msgs)))
			return
		}
		if err != nil {
			p.statusLbl.SetText(fmt.Sprintf("拉取不完整，收到 %d 条消息", len(msgs)))
			var partErrs core.PartitionErrors
			if errors.As(err, &partErrs) {
				showInfo(p.win, "部分分区拉取失败", partErrs.Error())
			} else {
				showError(p.win, "拉取消息失败", err)
			}
			return
		}
		if len(msgs) == 0 {
			p.allRows = nil
			p.applyMsgFilter()
		}
		if len(p.allRows) == len(p.rows) {
			p.statusLbl.SetText(fmt.Sprintf("共 %d 条消息", len(msgs)))
		}
	})
}

// showMessageDetail 弹出消息详情对话框，展示 Key/Value/Headers。
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

// onProduce 弹出「发送消息」对话框。
func (p *messagesPanel) onProduce() {
	if len(p.topics) == 0 {
		showInfo(p.win, "提示", "请先刷新 Topic 列表")
		return
	}
	topicSel := newFilterSelect("筛选 Topic", nil)
	topicSel.SetOptions(p.topics)
	topicSel.SetSelected(p.topicSel.Selected())
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
		{Text: "Topic", Widget: topicSel.object()},
		{Text: "分区 (留空或-1为自动)", Widget: partE},
		{Text: "Key", Widget: keyE},
		{Text: "Value", Widget: valE},
		{Text: "Headers", Widget: headersE},
	}
	var d dialog.Dialog
	d = dialog.NewForm("发送消息", "发送", "取消", items, func(ok bool) {
		if !ok {
			return
		}
		topic := topicSel.Selected()
		if topic == "" {
			showInfo(p.win, "提示", "请选择 Topic")
			d.Show()
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

// exportedMessage 用于 JSON 导出的消息结构。
// 字段与 MessageInfo 对齐，Key/Value 使用 FormatPayload 格式化。
type exportedMessage struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp string            `json:"timestamp"`
	Size      int               `json:"size"`
}

// onExport 导出当前过滤后的消息为 JSON 文件。
func (p *messagesPanel) onExport() {
	if len(p.allRows) == 0 {
		showInfo(p.win, "提示", "没有可导出的消息，请先拉取消息")
		return
	}
	rows := p.rows
	if len(rows) == 0 {
		showInfo(p.win, "提示", "当前过滤结果为空，请调整过滤条件")
		return
	}

	dlg := dialog.NewFileSave(func(writer fyne.URIWriteCloser, err error) {
		if err != nil {
			showError(p.win, "导出失败", err)
			return
		}
		if writer == nil {
			return // 用户取消
		}
		defer writer.Close()

		exported := make([]exportedMessage, 0, len(rows))
		for _, m := range rows {
			em := exportedMessage{
				Topic:     m.Topic,
				Partition: m.Partition,
				Offset:    m.Offset,
				Key:       core.FormatPayload(m.Key),
				Value:     core.FormatPayload(m.Value),
				Timestamp: "",
				Size:      m.Size,
			}
			if !m.Timestamp.IsZero() {
				em.Timestamp = m.Timestamp.Format("2006-01-02 15:04:05")
			}
			if len(m.Headers) > 0 {
				em.Headers = make(map[string]string, len(m.Headers))
				for _, h := range m.Headers {
					em.Headers[h.Key] = h.Value
				}
			}
			exported = append(exported, em)
		}

		data, err := json.MarshalIndent(exported, "", "  ")
		if err != nil {
			showError(p.win, "序列化失败", err)
			return
		}
		if _, err := writer.Write(data); err != nil {
			showError(p.win, "写入文件失败", err)
			return
		}
		showInfo(p.win, "导出成功", fmt.Sprintf("已导出 %d 条消息", len(exported)))
	}, p.win)
	dlg.SetFileName("messages.json")
	dlg.Resize(fyne.NewSize(600, 400))
	dlg.Show()
}

// parseTimeInput 解析用户输入的时间字符串，支持多种格式。
// 支持的格式：2026-08-26 12:00:05、2026-08-26 12:00、2026-08-26、2026/08/26 12:00:05 等。
func parseTimeInput(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
		"2006/01/02 15:04:05",
		"2006/01/02 15:04",
		"2006/01/02",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析时间: %s", s)
}
