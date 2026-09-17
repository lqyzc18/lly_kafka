// Package ui 实现 LlyKafka 的 fyne 桌面界面。
package ui

import (
	"context"
	"fmt"
	"image/color"
	"os"
	"strings"

	"lly_kafka/internal/core"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gfile"
)

var cjkFont fyne.Resource

func loadCJKFont() fyne.Resource {
	candidates := make([]string, 0, 8)
	if p := strings.TrimSpace(os.Getenv("FYNE_FONT")); p != "" {
		candidates = append(candidates, p)
	}
	candidates = append(candidates,
		"C:/Windows/Fonts/simhei.ttf",
		"C:/Windows/Fonts/msyh.ttc",
		"C:/Windows/Fonts/simsun.ttc",
		"/System/Library/Fonts/PingFang.ttc",
		"/System/Library/Fonts/STHeiti Light.ttc",
		"/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
		"/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
	)
	for _, p := range candidates {
		if !gfile.Exists(p) {
			continue
		}
		data := gfile.GetBytes(p)
		if len(data) > 0 {
			g.Log().Infof(core.Ctx, "使用中文字体: %s (%d bytes)", p, len(data))
			return &fyne.StaticResource{StaticName: "cjk-font", StaticContent: data}
		}
	}
	g.Log().Warning(core.Ctx, "未找到系统中文字体，中文显示可能为方块。设置 FYNE_FONT 环境变量或安装 simhei.ttf。")
	return nil
}

func startCJKFontLoader(a fyne.App, refresh func()) {
	go func() {
		font := loadCJKFont()
		if font == nil {
			return
		}
		fyne.Do(func() {
			cjkFont = font
			applyCJKTheme(a)
			if refresh != nil {
				refresh()
			}
		})
	}()
}

type cjkTheme struct{ base fyne.Theme }

func (t cjkTheme) Color(name fyne.ThemeColorName, variant fyne.ThemeVariant) color.Color {
	return t.base.Color(name, variant)
}
func (t cjkTheme) Icon(name fyne.ThemeIconName) fyne.Resource {
	return t.base.Icon(name)
}
func (t cjkTheme) Size(name fyne.ThemeSizeName) float32 {
	return t.base.Size(name)
}
func (t cjkTheme) Font(style fyne.TextStyle) fyne.Resource {
	if cjkFont != nil {
		return cjkFont
	}
	return t.base.Font(style)
}

func applyCJKTheme(a fyne.App) {
	a.Settings().SetTheme(cjkTheme{base: theme.DefaultTheme()})
}

// showError 弹出错误对话框并记录日志。
func showError(w fyne.Window, title string, err error) {
	if err == nil {
		return
	}
	g.Log().Errorf(core.Ctx, "%s: %v", title, err)
	dialog.ShowError(fmt.Errorf("%s: %v", title, err), w)
}

// showInfo 弹出信息对话框。
func showInfo(w fyne.Window, title, msg string) {
	dialog.ShowInformation(title, msg, w)
}

// clusterLabel 返回可展示的集群标识；旧版本 broker 可能不返回集群 ID。
func clusterLabel(id string) string {
	if strings.TrimSpace(id) == "" {
		return "(未知)"
	}
	return id
}

// fetchAndShow 异步加载数据并在 UI 线程显示结果。
// ctx 用于在客户端关闭后丢弃过期回调。
func fetchAndShow[T any](ctx context.Context, load func() (T, error), show func(val T, err error)) {
	go func() {
		val, err := load()
		fyne.Do(func() {
			if ctx != nil && ctx.Err() != nil {
				return
			}
			show(val, err)
		})
	}()
}

// previewBytes 将字节切片转为可显示的预览字符串。
// 超过 max 字符截断，控制字符转为转义序列，二进制数据转为 hex。
func previewBytes(b []byte, max int) string {
	if b == nil {
		return "null"
	}
	var b2 strings.Builder
	n := 0
	truncated := false
	for _, r := range string(b) {
		if max > 0 && n >= max {
			truncated = true
			break
		}
		n++
		switch r {
		case '\n':
			b2.WriteString(`\n`)
		case '\r':
			b2.WriteString(`\r`)
		case '\t':
			b2.WriteString(`\t`)
		case 0:
			b2.WriteString(`\0`)
		default:
			if r < 32 {
				b2.WriteString("·")
			} else {
				b2.WriteRune(r)
			}
		}
	}
	if truncated {
		b2.WriteRune('…')
	}
	return b2.String()
}
