package ui

import (
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// filterSelect 带筛选的下拉选择，选项很多时先输入再定位。
type filterSelect struct {
	all      []string
	selected string
	filter   *widget.Entry
	sel      *widget.Select
	onChange func(string)
	updating bool
}

func newFilterSelect(placeholder string, onChange func(string)) *filterSelect {
	f := &filterSelect{onChange: onChange, all: []string{}}
	f.filter = widget.NewEntry()
	f.filter.SetPlaceHolder(placeholder)
	f.sel = widget.NewSelect(nil, func(s string) {
		if f.updating || s == "" {
			return
		}
		if f.selected == s {
			return
		}
		f.selected = s
		if f.onChange != nil {
			f.onChange(s)
		}
	})
	f.sel.PlaceHolder = "请选择"
	f.filter.OnChanged = func(string) { f.syncOptions() }
	f.filter.OnSubmitted = func(string) { f.pickUniqueOrExact() }
	return f
}

func (f *filterSelect) object() fyne.CanvasObject {
	return container.NewVBox(f.filter, f.sel)
}

func (f *filterSelect) filterWidget() fyne.CanvasObject {
	return f.filter
}

func (f *filterSelect) selectWidget() fyne.CanvasObject {
	return f.sel
}

func (f *filterSelect) Selected() string {
	return f.selected
}

func (f *filterSelect) SetOptions(names []string) {
	f.all = append([]string(nil), names...)
	if f.selected != "" && !contains(f.all, f.selected) {
		f.selected = ""
	}
	f.syncOptions()
}

func (f *filterSelect) SetSelected(name string) {
	if name != "" && len(f.all) > 0 && !contains(f.all, name) {
		return
	}
	changed := f.selected != name
	f.selected = name
	f.syncOptions()
	if changed && name != "" && f.onChange != nil {
		f.onChange(name)
	}
}

func (f *filterSelect) syncOptions() {
	opts := filterTopics(f.all, f.filter.Text)
	f.updating = true
	f.sel.SetOptions(opts)
	switch {
	case f.selected != "" && contains(opts, f.selected):
		f.sel.PlaceHolder = "请选择"
		f.sel.SetSelected(f.selected)
	default:
		if len(opts) == 0 && strings.TrimSpace(f.filter.Text) != "" {
			f.sel.PlaceHolder = "无匹配"
		} else {
			f.sel.PlaceHolder = "请选择"
		}
		f.sel.ClearSelected()
		f.sel.Refresh()
	}
	f.updating = false
}

func (f *filterSelect) pickUniqueOrExact() {
	q := strings.TrimSpace(f.filter.Text)
	opts := filterTopics(f.all, q)
	if q != "" {
		for _, o := range opts {
			if strings.EqualFold(o, q) || o == q {
				f.SetSelected(o)
				return
			}
		}
	}
	if len(opts) == 1 {
		f.SetSelected(opts[0])
	}
}
