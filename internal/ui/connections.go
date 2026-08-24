package ui

import (
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"lly_kafka/internal/core"
)

// 常用 Kafka 版本选项（供新建/编辑连接时选择）。
var versionOptions = []string{
	"4.0.0",
	"3.9.0",
	"3.6.0",
	"3.3.0",
	"3.0.0",
	"2.8.0",
	"2.6.0",
	"2.0.0",
	"0.11.0",
	"自动 (2.6.0)",
}

// connectionDialog 打开「新建/编辑连接」对话框。
// editing 为 nil 时表示新建，否则为编辑模式（按 Name 预填）。
func (mw *MainWindow) connectionDialog(editing *core.Connection) {
	nameE := widget.NewEntry()
	nameE.SetPlaceHolder("连接名称")
	brokersE := widget.NewEntry()
	brokersE.SetPlaceHolder("127.0.0.1:9092,192.168.1.10:9092")
	versionSel := widget.NewSelect(versionOptions, nil)
	clientIDE := widget.NewEntry()
	clientIDE.SetPlaceHolder("lly-kafka")
	remarkE := widget.NewEntry()
	remarkE.SetPlaceHolder("备注（可选）")

	if editing != nil {
		nameE.SetText(editing.Name)
		nameE.Disable() // 名称作为主键不可修改
		brokersE.SetText(editing.Brokers)
		if editing.Version == "" {
			versionSel.SetSelected("自动 (2.6.0)")
		} else {
			versionSel.SetSelected(editing.Version)
		}
		clientIDE.SetText(editing.ClientID)
		remarkE.SetText(editing.Remark)
	} else {
		brokersE.SetText(mw.cfg.DefaultBrokers)
		versionSel.SetSelected("自动 (2.6.0)")
		clientIDE.SetText("lly-kafka")
	}

	formItems := []*widget.FormItem{
		{Text: "名称", Widget: nameE},
		{Text: "Broker 地址", Widget: brokersE},
		{Text: "Kafka 版本", Widget: versionSel},
		{Text: "Client ID", Widget: clientIDE},
		{Text: "备注", Widget: remarkE},
	}
	title := "新建连接"
	if editing != nil {
		title = "编辑连接 - " + editing.Name
	}

	d := dialog.NewForm(title, "保存", "取消", formItems, func(ok bool) {
		if !ok {
			return
		}
		name := strings.TrimSpace(nameE.Text)
		brokers := strings.TrimSpace(brokersE.Text)
		if name == "" || brokers == "" {
			showInfo(mw.win, "提示", "名称和 Broker 地址不能为空")
			return
		}
		ver := versionSel.Selected
		if strings.HasPrefix(ver, "自动") || ver == "" {
			ver = "2.6.0"
		}
		conn := &core.Connection{
			Name:     name,
			Brokers:  brokers,
			Version:  ver,
			ClientID: strings.TrimSpace(clientIDE.Text),
			Remark:   remarkE.Text,
		}
		if err := mw.store.Upsert(conn); err != nil {
			showError(mw.win, "保存连接失败", err)
			return
		}
		mw.refreshConnSel()
	}, mw.win)
	d.Resize(fyne.NewSize(480, 360))
	d.Show()
}

// onNewConn 新建连接按钮。
func (mw *MainWindow) onNewConn() {
	mw.connectionDialog(nil)
}

// onEditConn 编辑当前选中的连接。
func (mw *MainWindow) onEditConn() {
	name := mw.connSel.Selected
	if name == "" {
		showInfo(mw.win, "提示", "请先选择一个连接")
		return
	}
	if c := mw.store.Get(name); c != nil {
		mw.connectionDialog(c)
	}
}

// onDeleteConn 删除当前选中的连接。
func (mw *MainWindow) onDeleteConn() {
	name := mw.connSel.Selected
	if name == "" {
		showInfo(mw.win, "提示", "请先选择一个连接")
		return
	}
	dialog.ShowConfirm("确认删除", "确定要删除连接「"+name+"」吗？", func(ok bool) {
		if !ok {
			return
		}
		if err := mw.store.Remove(name); err != nil {
			showError(mw.win, "删除失败", err)
			return
		}
		// 如果删除的是当前连接，先断开
		mw.mu.Lock()
		if mw.conn != nil && mw.conn.Name == name {
			if mw.client != nil {
				mw.client.Close()
			}
			mw.client, mw.conn = nil, nil
		}
		mw.mu.Unlock()
		mw.refreshConnSel()
		mw.status.SetText("未连接")
	}, mw.win)
}

// refreshConnSel 重建连接下拉选项并保持/恢复选中。
func (mw *MainWindow) refreshConnSel() {
	names := mw.store.Names()
	mw.connSel.Options = names
	if len(names) > 0 {
		// 尝试保留当前选择，否则选第一个
		if mw.connSel.Selected == "" || !contains(names, mw.connSel.Selected) {
			mw.connSel.SetSelected(names[0])
		} else {
			mw.connSel.Refresh()
		}
	} else {
		mw.connSel.SetSelected("")
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// connIcon 仅在 MainWindow.build 中用到，保持导入。
var _ = theme.MediaPlayIcon