package ui

import (
	"fmt"
	"strconv"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/os/gtime"
	"lly_kafka/internal/core"
)

// messagesPanel 消息浏览与发送 Tab。
type messagesPanel struct {
	win    fyne.Window
	client *core.KafkaClient
	cfg    *core.AppConfig

	topics   []string
	topicSel *widget.Select
	partSel  *widget.Select

	mode      *widget.RadioGroup
	countE    *widget.Entry
	startE    *widget.Entry
	endE      *widget.Entry
	btnFetch  *widget.Button
	btnProduce *widget.Button
	statusLbl *widget.Label

	table *widget.Table
	rows  []core.MessageInfo
}

// newMessagesPanel 构造消息 Tab。
func newMessagesPanel(win fyne.Window, client *core.KafkaClient, cfg *core.AppConfig) *messagesPanel {
	return &messagesPanel{win: win, client: client, cfg: cfg, rows: []core.MessageInfo{}}
}

func (p *messagesPanel) build() fyne.CanvasObject {
	p.topicSel = widget.NewSelect([]string{}, func(string) {
		p.refreshPartitions()
	})
	p.partSel = widget.NewSelect([]string{}, func(string) {})

	p.mode = widget.NewRadioGroup([]string{"最新 N 条", "指定 Offset 范围"}, func(string) {
		p.updateModeFields()
	})
	p.mode.Horizontal = true
	p.mode.SetSelected("最新 N 条")

	p.countE = widget.NewEntry()
	p.countE.SetText(strconv.Itoa(p.cfg.DefaultMessageCount))
	p.startE = widget.NewEntry()
	p.startE.SetText("0")
	p.startE.Disable()
	p.endE = widget.NewEntry()
	p.endE.SetText("0")
	p.endE.Disable()

	p.btnFetch = widget.NewButtonWithIcon("拉取消息", theme.SearchIcon(), p.onFetch)
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
		container.NewHBox(p.btnFetch, p.btnProduce, p.statusLbl),
	)

	scroll := container.NewVScroll(p.table)
	scroll.SetMinSize(fyne.NewSize(600, 400))

	return container.NewBorder(form, nil, nil, nil, scroll)
}

// updateCell 表格单元格渲染。
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

// updateModeFields 根据模式启用/禁用输入。
func (p *messagesPanel) updateModeFields() {
	if p.mode.Selected == "最新 N 条" {
		p.countE.Enable()
		p.startE.Disable()
		p.endE.Disable()
	} else {
		p.countE.Disable()
		p.startE.Enable()
		p.endE.Enable()
	}
}

// refresh 拉取 Topic 列表并选中第一个。
func (p *messagesPanel) refresh() {
	go func() {
		names, err := p.client.ListTopicNames(core.Ctx)
		fyne.Do(func() {
			if err != nil {
				showError(p.win, "获取 Topic 失败", err)
				return
			}
			p.topics = names
			p.topicSel.Options = names
			if len(names) > 0 {
				p.topicSel.SetSelected(names[0])
				p.refreshPartitions()
			} else {
				p.topicSel.SetSelected("")
				p.partSel.Options = []string{}
				p.partSel.SetSelected("")
			}
		})
	}()
}

// refreshPartitions 根据当前 Topic 更新分区选项。
func (p *messagesPanel) refreshPartitions() {
	name := p.topicSel.Selected
	if name == "" {
		return
	}
	go func() {
		n, err := p.client.PartitionCount(core.Ctx, name)
		fyne.Do(func() {
			if err != nil {
				showError(p.win, "获取分区数失败", err)
				return
			}
			opts := make([]string, n)
			for i := 0; i < n; i++ {
				opts[i] = strconv.Itoa(i)
			}
			p.partSel.Options = opts
			if n > 0 {
				p.partSel.SetSelected(opts[0])
			}
		})
	}()
}

// onFetch 拉取消息。
func (p *messagesPanel) onFetch() {
	topic := p.topicSel.Selected
	partStr := p.partSel.Selected
	if topic == "" || partStr == "" {
		showInfo(p.win, "提示", "请选择 Topic 和分区")
		return
	}
	part := int32(gconv.Int(partStr))
	var start, end int64
	if p.mode.Selected == "最新 N 条" {
		n := int64(gconv.Int(p.countE.Text))
		if n <= 0 {
			n = int64(p.cfg.DefaultMessageCount)
		}
		hwm, err := p.client.NewestOffset(core.Ctx, topic, part)
		if err != nil {
			showError(p.win, "获取最新 offset 失败", err)
			return
		}
		start = hwm - n
		if start < 0 {
			start = 0
		}
		end = 0
	} else {
		start = gconv.Int64(p.startE.Text)
		end = gconv.Int64(p.endE.Text)
		if end > 0 && end < start {
			showInfo(p.win, "提示", "结束 offset 应大于起始 offset")
			return
		}
	}
	p.statusLbl.SetText("拉取中...")
	go func() {
		msgs, err := p.client.FetchMessages(core.Ctx, topic, part, start, end, p.cfg.FetchMaxMessages)
		fyne.Do(func() {
			if err != nil {
				p.statusLbl.SetText("拉取失败")
				showError(p.win, "拉取消息失败", err)
				return
			}
			p.rows = msgs
			p.statusLbl.SetText(fmt.Sprintf("共 %d 条消息", len(msgs)))
			p.table.Refresh()
		})
	}()
}

// showMessageDetail 弹窗显示完整消息内容。
func (p *messagesPanel) showMessageDetail(m core.MessageInfo) {
	keyE := widget.NewEntry()
	keyE.SetText(string(m.Key))
	keyE.MultiLine = true
	keyE.Wrapping = fyne.TextWrapWord
	valE := widget.NewEntry()
	valE.SetText(string(m.Value))
	valE.MultiLine = true
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

	content := container.NewVBox(
		widget.NewLabelWithStyle(fmt.Sprintf("Topic: %s   分区: %d   Offset: %d   大小: %d",
			m.Topic, m.Partition, m.Offset, m.Size), fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Key:"), keyE,
		widget.NewLabel("Value:"), valE,
		hdrLbl,
	)
	dlg := dialog.NewCustom("消息详情", "关闭", content, p.win)
	dlg.Resize(fyne.NewSize(700, 500))
	dlg.Show()
}

// onProduce 弹出发送消息对话框。
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
		var hdrs map[string]string
		if raw := strings.TrimSpace(headersE.Text); raw != "" {
			hdrs = map[string]string{}
			for _, kv := range strings.Split(raw, ",") {
				if idx := strings.Index(kv, "="); idx > 0 {
					hdrs[strings.TrimSpace(kv[:idx])] = strings.TrimSpace(kv[idx+1:])
				}
			}
		}
		go func() {
			pid, offset, err := p.client.ProduceMessage(core.Ctx, topic, int32(gconv.Int(partE.Text)),
				[]byte(keyE.Text), []byte(valE.Text), hdrs)
			fyne.Do(func() {
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