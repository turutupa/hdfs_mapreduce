package helpers

import (
	"fmt"
	"github.com/common-nighthawk/go-figure"
)

func PrintTitle(title string) {
	fmt.Println()
	figure.NewFigure(title, "small", true).Print()
	fmt.Println()
}
