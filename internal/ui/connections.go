package ui

import (
	"fmt"
	"strings"

	"lly_kafka/internal/core"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
)

// versionAuto 表示不指定版本（franz-go 自动协商）。
const versionAuto = "自动协商"

// versionOptions Kafka 版本下拉选项。
// franz-go 会自动协商协议版本，该字段仅作为连接的备注信息保存，不影响连接行为。
var versionOptions = []string{
	versionAuto,
	"4.0.0",
	"3.9.0",
	"3.6.0",
	"3.3.0",
	"3.0.0",
	"2.8.0",
	"2.6.0",
	"2.0.0",
	"0.11.0",
}

// securityOptions 安全协议下拉选项。
var securityOptions = []string{
	core.SecurityPlaintext,
	core.SecuritySASLPlaintext,
	core.SecuritySSL,
	core.SecuritySASLSSL,
}

// saslOptions SASL 机制下拉选项。
var saslOptions = []string{
	core.SASLPlain,
	core.SASLScramSHA256,
	core.SASLScramSHA512,
}

// connectionDialog 弹出连接配置对话框（新建或编辑）。
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

	secSel := widget.NewSelect(securityOptions, nil)
	saslSel := widget.NewSelect(saslOptions, nil)
	userE := widget.NewEntry()
	userE.SetPlaceHolder("SASL 用户名")
	passE := widget.NewPasswordEntry()
	passE.SetPlaceHolder("SASL 密码（保存在本地 connections.json）")
	skipTLS := widget.NewCheck("跳过证书校验（仅测试环境）", nil)
	caE := widget.NewEntry()
	caE.SetPlaceHolder("CA 证书路径（可选）")
	certE := widget.NewEntry()
	certE.SetPlaceHolder("客户端证书路径（可选）")
	keyE := widget.NewEntry()
	keyE.SetPlaceHolder("客户端私钥路径（可选）")
	updateSecurityFields := func(sec string) {
		sasl := sec == core.SecuritySASLPlaintext || sec == core.SecuritySASLSSL
		tls := sec == core.SecuritySSL || sec == core.SecuritySASLSSL
		if sasl {
			saslSel.Enable()
			userE.Enable()
			passE.Enable()
		} else {
			saslSel.Disable()
			userE.Disable()
			passE.Disable()
		}
		if tls {
			skipTLS.Enable()
			caE.Enable()
			certE.Enable()
			keyE.Enable()
		} else {
			skipTLS.Disable()
			caE.Disable()
			certE.Disable()
			keyE.Disable()
		}
	}
	secSel.OnChanged = updateSecurityFields

	if editing != nil {
		nameE.SetText(editing.Name)
		nameE.Disable()
		brokersE.SetText(editing.Brokers)
		if editing.Version == "" {
			versionSel.SetSelected(versionAuto)
		} else {
			versionSel.SetSelected(editing.Version)
		}
		clientIDE.SetText(editing.ClientID)
		remarkE.SetText(editing.Remark)
		if editing.Security == "" {
			secSel.SetSelected(core.SecurityPlaintext)
		} else {
			secSel.SetSelected(editing.Security)
		}
		if editing.SASLMechanism == "" {
			saslSel.SetSelected(core.SASLPlain)
		} else {
			saslSel.SetSelected(editing.SASLMechanism)
		}
		userE.SetText(editing.SASLUsername)
		passE.SetText(editing.SASLPassword)
		skipTLS.SetChecked(editing.TLSInsecure)
		caE.SetText(editing.TLSCAFile)
		certE.SetText(editing.TLSCertFile)
		keyE.SetText(editing.TLSKeyFile)
	} else {
		brokersE.SetText(mw.cfg.DefaultBrokers)
		versionSel.SetSelected(versionAuto)
		clientIDE.SetText("lly-kafka")
		secSel.SetSelected(core.SecurityPlaintext)
		saslSel.SetSelected(core.SASLPlain)
	}
	updateSecurityFields(secSel.Selected)

	collect := func() *core.Connection {
		ver := versionSel.Selected
		if strings.HasPrefix(ver, "自动") || ver == "" {
			// franz-go 自动协商，不把「自动」写成具体版本号，避免用户误以为需要指定版本。
			ver = ""
		}
		conn := &core.Connection{
			Name:          strings.TrimSpace(nameE.Text),
			Brokers:       strings.TrimSpace(brokersE.Text),
			Version:       ver,
			ClientID:      strings.TrimSpace(clientIDE.Text),
			Remark:        remarkE.Text,
			Security:      secSel.Selected,
			SASLMechanism: saslSel.Selected,
			SASLUsername:  strings.TrimSpace(userE.Text),
			SASLPassword:  passE.Text,
			TLSInsecure:   skipTLS.Checked,
			TLSCAFile:     strings.TrimSpace(caE.Text),
			TLSCertFile:   strings.TrimSpace(certE.Text),
			TLSKeyFile:    strings.TrimSpace(keyE.Text),
		}
		if conn.Security != core.SecuritySASLPlaintext && conn.Security != core.SecuritySASLSSL {
			conn.SASLMechanism, conn.SASLUsername, conn.SASLPassword = "", "", ""
		}
		if conn.Security != core.SecuritySSL && conn.Security != core.SecuritySASLSSL {
			conn.TLSInsecure = false
			conn.TLSCAFile, conn.TLSCertFile, conn.TLSKeyFile = "", "", ""
		}
		return conn
	}

	var testBtn *widget.Button
	testBtn = widget.NewButton("测试连接", func() {
		c := collect()
		if c.Name == "" || c.Brokers == "" {
			showInfo(mw.win, "提示", "请先填写名称和 Broker 地址")
			return
		}
		testCtx, seq := mw.beginTest()
		testBtn.Disable()
		go func() {
			clusterID, n, err := core.TestConnection(testCtx, c)
			fyne.Do(func() {
				if !mw.testCurrent(seq) {
					return
				}
				testBtn.Enable()
				if err != nil {
					showError(mw.win, "测试失败", err)
					return
				}
				showInfo(mw.win, "测试成功", fmt.Sprintf("集群 %s · %d brokers", clusterLabel(clusterID), n))
			})
		}()
	})

	formItems := []*widget.FormItem{
		{Text: "名称", Widget: nameE},
		{Text: "Broker 地址", Widget: brokersE},
		{Text: "版本(仅备注)", Widget: versionSel},
		{Text: "Client ID", Widget: clientIDE},
		{Text: "安全协议", Widget: secSel},
		{Text: "SASL 机制", Widget: saslSel},
		{Text: "用户名", Widget: userE},
		{Text: "密码", Widget: passE},
		{Text: "TLS", Widget: skipTLS},
		{Text: "CA 证书", Widget: caE},
		{Text: "客户端证书", Widget: certE},
		{Text: "客户端私钥", Widget: keyE},
		{Text: "备注", Widget: remarkE},
		{Text: "", Widget: testBtn},
	}
	title := "新建连接"
	if editing != nil {
		title = "编辑连接 - " + editing.Name
	}

	var d dialog.Dialog
	d = dialog.NewForm(title, "保存", "取消", formItems, func(ok bool) {
		if !ok {
			return
		}
		conn := collect()
		if conn.Name == "" || conn.Brokers == "" {
			showInfo(mw.win, "提示", "名称和 Broker 地址不能为空")
			d.Show()
			return
		}
		if err := mw.store.Upsert(conn); err != nil {
			showError(mw.win, "保存连接失败", err)
			d.Show()
			return
		}
		reconnect := false
		mw.mu.Lock()
		reconnect = mw.client != nil && mw.conn != nil && mw.conn.Name == conn.Name
		mw.mu.Unlock()
		mw.refreshConnSel()
		if reconnect {
			mw.connSel.SetSelected(conn.Name)
			mw.onConnect()
		}
	}, mw.win)
	d.Resize(fyne.NewSize(560, 640))
	d.Show()
}

// onNewConn 处理「新建连接」按钮点击。
func (mw *MainWindow) onNewConn() {
	mw.connectionDialog(nil)
}

// onEditConn 处理「编辑连接」按钮点击，编辑当前选中的连接。
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

// onTestConn 处理「测试」按钮点击，测试当前选中的连接。
func (mw *MainWindow) onTestConn() {
	name := mw.connSel.Selected
	if name == "" {
		showInfo(mw.win, "提示", "请先选择一个连接")
		return
	}
	conn := mw.store.Get(name)
	if conn == nil {
		showInfo(mw.win, "提示", "未找到连接配置: "+name)
		return
	}
	mw.setStatus("测试连接中...", widget.WarningImportance)
	testCtx, seq := mw.beginTest()
	go func() {
		clusterID, n, err := core.TestConnection(testCtx, conn)
		fyne.Do(func() {
			if !mw.testCurrent(seq) {
				return
			}
			if !mw.connected() {
				if err != nil {
					mw.setStatus("未连接", widget.DangerImportance)
				} else {
					mw.setStatus("未连接", widget.MediumImportance)
				}
			}
			if err != nil {
				showError(mw.win, "测试失败", err)
				return
			}
			showInfo(mw.win, "测试成功", fmt.Sprintf("%s · 集群 %s · %d brokers", conn.Name, clusterLabel(clusterID), n))
		})
	}()
}

// onDeleteConn 处理「删除」按钮点击，删除当前选中的连接。
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
		wasCurrent := false
		mw.mu.Lock()
		if mw.conn != nil && mw.conn.Name == name {
			wasCurrent = true
			mw.bumpSeqLocked()
			old := mw.takeClientLocked()
			mw.mu.Unlock()
			if old != nil {
				go old.Close()
			}
		} else {
			mw.mu.Unlock()
		}
		mw.refreshConnSel()
		if wasCurrent {
			mw.showDisconnected()
		}
	}, mw.win)
}

// refreshConnSel 刷新连接下拉列表，保持当前选中或回退到第一个。
func (mw *MainWindow) refreshConnSel() {
	names := mw.store.Names()
	mw.connSel.Options = names
	if len(names) > 0 {
		if mw.connSel.Selected == "" || !contains(names, mw.connSel.Selected) {
			mw.connSel.SetSelected(names[0])
		} else {
			mw.connSel.Refresh()
		}
	} else {
		mw.connSel.SetSelected("")
	}
}
