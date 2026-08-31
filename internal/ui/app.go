package ui

import (
	"context"
	"fmt"
	"sync"

	"lly_kafka/internal/core"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/gogf/gf/v2/frame/g"
)

type MainWindow struct {
	fyneApp fyne.App
	win     fyne.Window
	cfg     *core.AppConfig
	store   *core.ConnectionStore

	mu          sync.Mutex
	connectSeq  uint64
	testSeq     uint64
	client      *core.KafkaClient
	conn        *core.Connection
	windowCtx   context.Context
	windowStop  context.CancelFunc
	connectStop context.CancelFunc

	root    *fyne.Container
	toolbar *fyne.Container
	body    *fyne.Container

	connSel *widget.Select
	status  *widget.Label
	connBtn *widget.Button
	discBtn *widget.Button
}

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

	app.SetMetadata(fyne.AppMetadata{
		ID:      "com.lly.kafka",
		Name:    cfg.Name,
		Version: cfg.Version,
		Migrations: map[string]bool{
			"fyneDo": true,
		},
	})

	w := a.NewWindow(fmt.Sprintf("%s v%s - Kafka 管理工具", cfg.Name, cfg.Version))
	pref := a.Preferences()
	ww, hh := float32(pref.Float("win_w")), float32(pref.Float("win_h"))
	if ww < 900 {
		ww = 1280
	}
	if hh < 600 {
		hh = 800
	}
	w.Resize(fyne.NewSize(ww, hh))

	mw := &MainWindow{
		fyneApp: a,
		win:     w,
		cfg:     cfg,
		store:   store,
	}
	mw.windowCtx, mw.windowStop = context.WithCancel(context.Background())
	mw.build()
	w.SetContent(mw.root)
	startCJKFontLoader(a, func() {
		if w.Content() != nil {
			w.Content().Refresh()
		}
	})
	w.SetOnClosed(func() {
		sz := w.Canvas().Size()
		pref.SetFloat("win_w", float64(sz.Width))
		pref.SetFloat("win_h", float64(sz.Height))
		if mw.connSel != nil && mw.connSel.Selected != "" {
			pref.SetString("last_conn", mw.connSel.Selected)
		}
		mw.mu.Lock()
		mw.connectSeq++
		mw.testSeq++
		if mw.connectStop != nil {
			mw.connectStop()
			mw.connectStop = nil
		}
		c := mw.client
		mw.client, mw.conn = nil, nil
		mw.mu.Unlock()
		mw.windowStop()
		if c != nil {
			c.Close()
		}
	})
	w.ShowAndRun()
}

func (mw *MainWindow) build() {
	mw.connSel = widget.NewSelect(mw.store.Names(), nil)
	names := mw.store.Names()
	if last := mw.fyneApp.Preferences().String("last_conn"); last != "" && contains(names, last) {
		mw.connSel.SetSelected(last)
	} else if len(names) > 0 {
		mw.connSel.SetSelected(names[0])
	}
	mw.connBtn = widget.NewButtonWithIcon("连接", theme.MediaPlayIcon(), mw.onConnect)
	mw.discBtn = widget.NewButtonWithIcon("断开", theme.MediaStopIcon(), mw.onDisconnect)
	mw.discBtn.Disable()
	btnNew := widget.NewButtonWithIcon("新建连接", theme.ContentAddIcon(), mw.onNewConn)
	btnEdit := widget.NewButtonWithIcon("编辑", theme.DocumentCreateIcon(), mw.onEditConn)
	btnTest := widget.NewButtonWithIcon("测试", theme.ConfirmIcon(), mw.onTestConn)
	btnDel := widget.NewButtonWithIcon("删除", theme.DeleteIcon(), mw.onDeleteConn)

	mw.status = widget.NewLabel("未连接")
	mw.status.Importance = widget.MediumImportance

	left := container.NewHBox(
		widget.NewLabel("连接: "), mw.connSel,
		mw.connBtn, mw.discBtn,
		btnNew, btnEdit, btnTest, btnDel,
	)
	mw.toolbar = container.NewBorder(nil, nil, left, container.NewHBox(mw.status), nil)

	mw.body = container.NewStack(mw.placeholder())
	mw.root = container.NewBorder(mw.toolbar, nil, nil, nil, mw.body)
}

func (mw *MainWindow) placeholder() fyne.CanvasObject {
	icon := widget.NewIcon(theme.ComputerIcon())
	title := widget.NewLabelWithStyle("请先选择连接并点击「连接」", fyne.TextAlignCenter, fyne.TextStyle{Bold: true})
	hint := widget.NewLabelWithStyle("使用上方「新建连接」配置 Kafka 集群，支持 SASL / TLS。", fyne.TextAlignCenter, fyne.TextStyle{})
	return container.NewCenter(container.NewVBox(icon, title, hint))
}

func (mw *MainWindow) setStatus(text string, imp widget.Importance) {
	mw.status.SetText(text)
	mw.status.Importance = imp
	mw.status.Refresh()
}

func (mw *MainWindow) showDisconnected() {
	if mw.connSel != nil {
		mw.connSel.Enable()
	}
	mw.connBtn.Enable()
	mw.discBtn.Disable()
	mw.setStatus("未连接", widget.MediumImportance)
	mw.body.Objects = []fyne.CanvasObject{mw.placeholder()}
	mw.body.Refresh()
}

func (mw *MainWindow) bumpSeqLocked() uint64 {
	mw.connectSeq++
	return mw.connectSeq
}

func (mw *MainWindow) beginTest() (context.Context, uint64) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.testSeq++
	return mw.windowCtx, mw.testSeq
}

func (mw *MainWindow) testCurrent(seq uint64) bool {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	return mw.windowCtx.Err() == nil && mw.testSeq == seq
}

func (mw *MainWindow) connected() bool {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	return mw.client != nil
}

func (mw *MainWindow) takeClientLocked() *core.KafkaClient {
	c := mw.client
	mw.client, mw.conn = nil, nil
	return c
}

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

	mw.connBtn.Disable()
	if mw.connSel != nil {
		mw.connSel.Disable()
	}
	mw.discBtn.Enable()
	mw.setStatus("正在连接...", widget.WarningImportance)

	mw.mu.Lock()
	seq := mw.bumpSeqLocked()
	mw.testSeq++
	if mw.connectStop != nil {
		mw.connectStop()
	}
	connectCtx, connectStop := context.WithCancel(mw.windowCtx)
	mw.connectStop = connectStop
	old := mw.takeClientLocked()
	mw.mu.Unlock()
	if old != nil {
		go old.Close()
	}

	go func() {
		client, err := core.NewKafkaClient(connectCtx, conn)
		fyne.Do(func() {
			mw.mu.Lock()
			stale := mw.connectSeq != seq
			if mw.connectSeq == seq {
				mw.connectStop = nil
			}
			mw.mu.Unlock()
			if stale {
				connectStop()
				if client != nil {
					go client.Close()
				}
				return
			}
			if err != nil {
				connectStop()
				mw.connBtn.Enable()
				if mw.connSel != nil {
					mw.connSel.Enable()
				}
				mw.discBtn.Disable()
				mw.setStatus("未连接", widget.DangerImportance)
				showError(mw.win, "连接失败", err)
				return
			}
			mw.mu.Lock()
			mw.client = client
			mw.conn = conn
			mw.mu.Unlock()
			mw.connBtn.Enable()
			if mw.connSel != nil {
				mw.connSel.Enable()
			}
			mw.discBtn.Enable()
			sec := conn.Security
			if sec == "" {
				sec = core.SecurityPlaintext
			}
			mw.setStatus(fmt.Sprintf("已连接: %s | %s | Kafka %s | %d brokers",
				conn.Name, sec, client.KafkaVersion(client.Context()), client.BrokerCount(client.Context())), widget.SuccessImportance)
			mw.fyneApp.Preferences().SetString("last_conn", conn.Name)
			mw.refreshBody()
		})
	}()
}

func (mw *MainWindow) onDisconnect() {
	mw.mu.Lock()
	mw.bumpSeqLocked()
	mw.testSeq++
	if mw.connectStop != nil {
		mw.connectStop()
		mw.connectStop = nil
	}
	client := mw.takeClientLocked()
	mw.mu.Unlock()
	if client != nil {
		go client.Close()
	}
	mw.showDisconnected()
}

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

func (mw *MainWindow) buildTabs(client *core.KafkaClient) fyne.CanvasObject {
	connName := ""
	if mw.conn != nil {
		connName = mw.conn.Name
	}
	prefs := mw.fyneApp.Preferences()
	brokers := newBrokersPanel(mw.win, client)
	topics := newTopicsPanel(mw.win, client)
	msgs := newMessagesPanel(mw.win, client, mw.cfg, prefs, connName)
	groups := newGroupsPanel(mw.win, client)

	tabs := container.NewAppTabs(
		container.NewTabItem("集群", brokers.build()),
		container.NewTabItem("Topic", topics.build()),
		container.NewTabItem("消息", msgs.build()),
		container.NewTabItem("消费组", groups.build()),
	)
	tabs.OnSelected = func(item *container.TabItem) {
		switch item.Text {
		case "集群":
			brokers.ensureLoaded()
		case "Topic":
			topics.ensureLoaded()
		case "消息":
			msgs.ensureLoaded()
		case "消费组":
			groups.ensureLoaded()
		}
	}
	brokers.ensureLoaded()
	return tabs
}
