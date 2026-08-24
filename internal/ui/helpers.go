// Package ui 实现 LlyKafka 的 fyne 桌面界面。
// 该文件提供主题（CJK 字体）、通用对话框与字符串预览工具。
package ui

import (
	"fmt"
	"image/color"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gfile"
	"lly_kafka/internal/core"
)

// cjkFont 系统探测到的中文字体资源（启动时填充）。
var cjkFont fyne.Resource

func init() {
	cjkFont = loadCJKFont()
}

// loadCJKFont 按平台常见字体路径探测中文字体，使用 gf gfile 简化文件读取。
func loadCJKFont() fyne.Resource {
	candidates := []string{
		"C:/Windows/Fonts/simhei.ttf",
		"C:/Windows/Fonts/msyh.ttc",
		"C:/Windows/Fonts/simsun.ttc",
		"/System/Library/Fonts/PingFang.ttc",
		"/System/Library/Fonts/STHeiti Light.ttc",
		"/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
		"/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
	}
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

// cjkTheme 在默认主题上覆盖字体，以支持中文显示。
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

// applyCJKTheme 应用自定义主题（需在 NewWindow 之前调用）。
func applyCJKTheme(a fyne.App) {
	a.Settings().SetTheme(cjkTheme{base: theme.DefaultTheme()})
}

// showError 弹窗并记录日志。
func showError(w fyne.Window, title string, err error) {
	if err == nil {
		return
	}
	g.Log().Errorf(core.Ctx, "%s: %v", title, err)
	dialog.ShowError(fmt.Errorf("%s: %v", title, err), w)
}

// showInfo 普通信息提示弹窗。
func showInfo(w fyne.Window, title, msg string) {
	dialog.ShowInformation(title, msg, w)
}

// previewBytes 截取字节数组为可读字符串（用于表格预览）：
//   - 控制字符替换为转义形式（\n, \t, \r）
//   - 长度超过 max 则截断并追加 "…"
//   - nil 字节返回 "null"
func previewBytes(b []byte, max int) string {
	if b == nil {
		return "null"
	}
	s := string(b)
	if max <= 0 || len(s) <= max {
		max = len(s)
	} else {
		s = s[:max] + "…"
	}
	var b2 strings.Builder
	b2.Grow(len(s))
	for _, r := range s {
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
	return b2.String()
}