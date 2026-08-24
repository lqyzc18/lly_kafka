package ui

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"lly_kafka/internal/core"
)

// brokersPanel 集群 Broker 列表 Tab。
type brokersPanel struct {
	win    fyne.Window
	client *core.KafkaClient

	rows  []core.BrokerInfo
	table *widget.Table
	info  *widget.Label
}

func newBrokersPanel(win fyne.Window, client *core.KafkaClient) *brokersPanel {
	return &brokersPanel{win: win, client: client}
}

func (p *brokersPanel) build() fyne.CanvasObject {
	p.info = widget.NewLabel("加载中...")
	p.info.Alignment = fyne.TextAlignCenter

	p.table = widget.NewTable(
		func() (int, int) { return len(p.rows) + 1, 5 },
		func() fyne.CanvasObject { return widget.NewLabel("") },
		p.updateCell,
	)
	p.table.SetColumnWidth(0, 60)
	p.table.SetColumnWidth(1, 180)
	p.table.SetColumnWidth(2, 80)
	p.table.SetColumnWidth(3, 120)
	p.table.SetColumnWidth(4, 100)

	btn := widget.NewButtonWithIcon("刷新", theme.ViewRefreshIcon(), p.refresh)
	head := container.NewHBox(btn, widget.NewLabel("  "), p.info)

	scroll := container.NewVScroll(p.table)
	scroll.SetMinSize(fyne.NewSize(600, 400))
	return container.NewBorder(head, nil, nil, nil, scroll)
}

func (p *brokersPanel) updateCell(id widget.TableCellID, obj fyne.CanvasObject) {
	label := obj.(*widget.Label)
	label.TextStyle = fyne.TextStyle{}
	if id.Row == 0 {
		label.TextStyle = fyne.TextStyle{Bold: true}
		switch id.Col {
		case 0:
			label.SetText("ID")
		case 1:
			label.SetText("主机")
		case 2:
			label.SetText("端口")
		case 3:
			label.SetText("机架")
		case 4:
			label.SetText("控制器")
		}
		return
	}
	if id.Row-1 >= len(p.rows) {
		return
	}
	r := p.rows[id.Row-1]
	switch id.Col {
	case 0:
		label.SetText(fmt.Sprintf("%d", r.ID))
	case 1:
		label.SetText(r.Host)
	case 2:
		label.SetText(fmt.Sprintf("%d", r.Port))
	case 3:
		label.SetText(r.Rack)
	case 4:
		if r.Controller {
			label.Importance = widget.SuccessImportance
			label.SetText("✔ Controller")
		} else {
			label.SetText("")
		}
	}
}

// refresh 拉取 Broker 列表（异步）。
func (p *brokersPanel) refresh() {
	p.info.SetText("刷新中...")
	go func() {
		rows, err := p.client.ListBrokers(core.Ctx)
		fyne.Do(func() {
			if err != nil {
				p.info.SetText("获取失败")
				showError(p.win, "获取 Broker 失败", err)
				return
			}
			p.rows = rows
			p.info.SetText(fmt.Sprintf("共 %d 个 Broker | 协议版本: %s", len(rows), p.client.KafkaVersion()))
			p.table.Refresh()
		})
	}()
}