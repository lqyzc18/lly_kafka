package ui

import (
	"fmt"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/gogf/gf/v2/frame/g"
	"lly_kafka/internal/core"
)

// MainWindow 主窗口结构，持有 UI 与 core 的连接状态。
type MainWindow struct {
	fyneApp fyne.App
	win     fyne.Window
	cfg     *core.AppConfig
	store   *core.ConnectionStore

	mu     sync.Mutex
	client *core.KafkaClient
	conn   *core.Connection

	root    *fyne.Container
	toolbar *fyne.Container
	body    *fyne.Container

	connSel *widget.Select
	status  *widget.Label
	connBtn *widget.Button
	discBtn *widget.Button
}

// Run 启动应用：创建 fyne App、加载配置与连接存储、构建主窗口。
func Run() {
	a := app.NewWithID("com.lly.kafka")
	applyCJKTheme(a)

	cfg, err := core.LoadAppConfig(core.Ctx)
	if err != nil {
		g.Log().Fatal(core.Ctx, err)
	}
	store, err := core.NewConnectionStore(cfg.ConnectionsFile)
	if err != nil {
		g.Log().Fatal(core.Ctx, err)
	}

	w := a.NewWindow(fmt.Sprintf("%s v%s - Kafka 管理工具（简易版）", cfg.Name, cfg.Version))
	w.Resize(fyne.NewSize(1280, 800))

	mw := &MainWindow{
		fyneApp: a,
		win:     w,
		cfg:     cfg,
		store:   store,
	}
	mw.build()
	w.SetContent(mw.root)
	w.SetOnClosed(func() {
		mw.mu.Lock()
		defer mw.mu.Unlock()
		if mw.client != nil {
			mw.client.Close()
			mw.client = nil
		}
	})
	w.ShowAndRun()
}

// build 构建主窗口 UI。
func (mw *MainWindow) build() {
	// ---- 顶部工具条 ----
	mw.connSel = widget.NewSelect(mw.store.Names(), func(s string) {
		// 选择变化只更新连接配置，不主动连接
	})
	if names := mw.store.Names(); len(names) > 0 {
		mw.connSel.SetSelected(names[0])
	}
	mw.connBtn = widget.NewButtonWithIcon("连接", theme.MediaPlayIcon(), mw.onConnect)
	mw.discBtn = widget.NewButtonWithIcon("断开", theme.MediaStopIcon(), mw.onDisconnect)
	mw.discBtn.Disable()
	btnNew := widget.NewButtonWithIcon("新建连接", theme.ContentAddIcon(), mw.onNewConn)
	btnEdit := widget.NewButtonWithIcon("编辑", theme.DocumentCreateIcon(), mw.onEditConn)
	btnDel := widget.NewButtonWithIcon("删除", theme.DeleteIcon(), mw.onDeleteConn)

	mw.status = widget.NewLabel("未连接")
	mw.status.Importance = widget.MediumImportance

	left := container.NewHBox(
		widget.NewLabel("连接: "), mw.connSel,
		mw.connBtn, mw.discBtn,
		btnNew, btnEdit, btnDel,
	)
	right := container.NewHBox(mw.status)
	mw.toolbar = container.NewBorder(nil, nil, left, right, nil)
	// 通过容器让状态栏右对齐
	mw.toolbar = container.NewBorder(nil, nil, left, container.NewHBox(mw.status), nil)

	// ---- 主体 ----
	mw.body = container.NewStack(mw.placeholder())
	mw.root = container.NewBorder(mw.toolbar, nil, nil, nil, mw.body)
}

// placeholder 占位组件，在未连接时显示。
func (mw *MainWindow) placeholder() fyne.CanvasObject {
	icon := widget.NewIcon(theme.ComputerIcon())
	title := widget.NewLabelWithStyle("请先选择连接并点击「连接」", fyne.TextAlignCenter, fyne.TextStyle{Bold: true})
	hint := widget.NewLabelWithStyle("使用上方「新建连接」配置你的 Kafka 集群。", fyne.TextAlignCenter, fyne.TextStyle{})
	return container.NewCenter(container.NewVBox(icon, title, hint))
}

// onConnect 连接按钮处理。
func (mw *MainWindow) onConnect() {
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
	mw.status.SetText("正在连接...")
	mw.status.Importance = widget.WarningImportance
	go func() {
		client, err := core.NewKafkaClient(core.Ctx, conn)
		fyne.Do(func() {
			if err != nil {
				mw.status.SetText("未连接")
				mw.status.Importance = widget.DangerImportance
				showError(mw.win, "连接失败", err)
				return
			}
			mw.mu.Lock()
			if mw.client != nil {
				mw.client.Close()
			}
			mw.client = client
			mw.conn = conn
			mw.mu.Unlock()
			mw.discBtn.Enable()
			mw.status.SetText(fmt.Sprintf("已连接: %s | Kafka %s | %d brokers",
				conn.Name, client.KafkaVersion(), client.BrokerCount()))
			mw.status.Importance = widget.SuccessImportance
			mw.refreshBody()
		})
	}()
}

// onDisconnect 断开按钮处理。
func (mw *MainWindow) onDisconnect() {
	mw.mu.Lock()
	if mw.client != nil {
		mw.client.Close()
		mw.client, mw.conn = nil, nil
	}
	mw.mu.Unlock()
	mw.discBtn.Disable()
	mw.status.SetText("未连接")
	mw.status.Importance = widget.MediumImportance
	mw.body.Objects = []fyne.CanvasObject{mw.placeholder()}
	mw.body.Refresh()
}

// refreshBody 重建右侧 Tab 内容（连接后调用）。
func (mw *MainWindow) refreshBody() {
	mw.mu.Lock()
	client := mw.client
	mw.mu.Unlock()
	if client == nil {
		mw.body.Objects = []fyne.CanvasObject{mw.placeholder()}
	} else {
		mw.body.Objects = []fyne.CanvasObject{mw.buildTabs(client)}
	}
	mw.body.Refresh()
}

// buildTabs 构造主 Tab 容器。
func (mw *MainWindow) buildTabs(client *core.KafkaClient) fyne.CanvasObject {
	brokers := newBrokersPanel(mw.win, client)
	topics := newTopicsPanel(mw.win, client, mw.cfg)
	msgs := newMessagesPanel(mw.win, client, mw.cfg)
	groups := newGroupsPanel(mw.win, client)

	// 自动触发首屏刷新
	brokers.refresh()
	topics.refresh()
	msgs.refresh()
	groups.refresh()

	return container.NewAppTabs(
		container.NewTabItem("集群", brokers.build()),
		container.NewTabItem("Topic", topics.build()),
		container.NewTabItem("消息", msgs.build()),
		container.NewTabItem("消费组", groups.build()),
	)
}