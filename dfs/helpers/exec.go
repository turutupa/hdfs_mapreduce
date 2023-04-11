package helpers

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/sirupsen/logrus"
)

const UUID_CMD = "uuidgen"
const WHOAMI_CMD = "whoami"
const CLEAR_CMD = "clear"

func NewUUID() (string, error) {
	if uuid, ok := exec.Command(UUID_CMD).Output(); ok != nil {
		return "", ok
	} else {
		return strings.TrimSpace(string(uuid)), ok
	}
}

func GetHomeDir() string {
	user, _ := exec.Command(WHOAMI_CMD).Output()
	return "/home/" + strings.TrimSuffix(string(user), "\n") + "/"
}

func ClearTerminal() {
	cmd := exec.Command(CLEAR_CMD) //Linux only for the time being :(
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		logrus.Error(err.Error())
	}
}

func BuildGoPlugin(path string) error {
	if !isGoFile(path) {
		return errors.New("provided file is not a go file")
	}
	_, err := exec.Command(buildGoPluginCmd(path)).Output()
	if err != nil {
		return err
	}
	return nil
}

func buildGoPluginCmd(path string) string {
	return fmt.Sprintf(
		"go build -buildmode=plugin -o %s %s",
		getPluginName(path),
		path,
	)
}

func getPluginName(path string) string {
	return path[0:len(path)-3] + ".so"
}

func isGoFile(path string) bool {
	extension := path[len(path)-3:]
	return extension == ".go"
}
