package dpm

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-logr/logr"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// TODO: Make plugin server start retries configurable.
const (
	startPluginServerRetries   = 3
	startPluginServerRetryWait = 3 * time.Second
)

// Manager contains the main machinery of this framework. It uses user defined lister to monitor
// available resources and start/stop plugins accordingly. It also handles system signals and
// unexpected kubelet events.
type Manager struct {
	ctx           context.Context
	lister        ListerInterface
	readyNotifier chan<- struct{}
	onceReady     sync.Once
	log           logr.Logger
}

// NewManager is the canonical way of initializing Manager. User must provide ListerInterface
// implementation. Lister will provide information about handled resources, monitor their
// availability and provide method to spawn plugins that will handle found resources.
func NewManager(ctx context.Context, lister ListerInterface, readyNotifier chan<- struct{}, log logr.Logger) *Manager {
	dpm := &Manager{
		ctx:           ctx,
		lister:        lister,
		readyNotifier: readyNotifier,
		log:           log,
	}
	return dpm
}

// Run starts the Manager. It sets up the infrastructure and handles system signals, Kubelet socket
// watch and monitoring of available resources as well as starting and stoping of plugins.
func (dpm *Manager) Run() error {
	dpm.log.V(0).Info("Starting device plugin manager")

	// First important signal channel is the os signal channel. We only care about (somewhat) small
	// subset of available signals.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)

	// The other important channel is filesystem notification channel, responsible for watching
	// device plugin directory.
	dpm.log.V(3).Info("Registering for notifications of filesystem changes in device plugin directory")
	var (
		fsWatcher       *fsnotify.Watcher
		err             error
		fsWatcherEvents <-chan fsnotify.Event
		tickerChan      <-chan time.Time
		lastModTime     time.Time
		socketExists    bool
	)

	// one is to understand how to cacth the point where the device plugin is set good and to ping the odiglet about that
	// and the other is to understand how to return error currectly from the device plugin Run method

	fsWatcher, err = func() (*fsnotify.Watcher, error) {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			dpm.log.V(0).Info("Failed to watch device plugin path, falling back to polling", "error", err)
			return nil, err
		}
		if err := w.Add(pluginapi.DevicePluginPath); err != nil {
			dpm.log.V(0).Info("Failed to watch device plugin path, falling back to polling", "error", err)

			return nil, err
		}
		return w, nil
	}()

	defer func() {
		if fsWatcher != nil {
			fsWatcher.Close()
		}
	}()

	// if failed to setup fsnotify watcher, fallback to polling
	if err != nil {
		ticker := time.NewTicker(5 * time.Second)
		tickerChan = ticker.C
		defer ticker.Stop()
	} else {
		fsWatcherEvents = fsWatcher.Events
	}

	// Create list of running plugins and start Discover method of given lister. This method is
	// responsible of notifying manager about changes in available plugins.
	var pluginMap = make(map[string]devicePlugin)
	dpm.log.V(3).Info("Starting Discovery on new plugins")
	pluginsCh := make(chan PluginNameList)
	defer close(pluginsCh)
	go dpm.lister.Discover(pluginsCh)

	// Finally start a loop that will handle messages from opened channels.
	dpm.log.V(3).Info("Handling incoming signals")
	socketCheckFailures := 0
HandleSignals:
	for {
		select {
		case newPluginsList := <-pluginsCh:
			dpm.handleNewPlugins(pluginMap, newPluginsList)

		case event := <-fsWatcherEvents:
			if event.Name == pluginapi.KubeletSocket {
				dpm.log.V(3).Info("Received kubelet socket event: %s", event)
				if event.Op&fsnotify.Create == fsnotify.Create {
					dpm.startPluginServers(pluginMap)
				}
				// TODO: Kubelet doesn't really clean-up it's socket, so this is currently
				// manual-testing thing. Could we solve Kubelet deaths better?
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					dpm.stopPluginServers(pluginMap)
				}
			}

		case <-tickerChan:
			info, err := os.Stat(pluginapi.KubeletSocket)
			if err == nil {
				socketCheckFailures = 0
				modTime := info.ModTime()
				if !socketExists || modTime.After(lastModTime) {
					lastModTime = modTime
					socketExists = true
					dpm.log.V(3).Info("Detected modification or creation of: %s", pluginapi.KubeletSocket)
					dpm.startPluginServers(pluginMap)
				}
			} else {
				socketExists = false
				socketCheckFailures++

				switch {
				case errors.Is(err, fs.ErrNotExist):
					dpm.log.V(3).Info("Kubelet socket does not exist yet: %v", err)
				case errors.Is(err, fs.ErrPermission):
					dpm.log.Error(err, "Permission denied accessing kubelet socket: %v", pluginapi.KubeletSocket)
				default:
					dpm.log.Error(err, "Error stating kubelet socket: %v", pluginapi.KubeletSocket)
				}

				if socketCheckFailures >= 5 {
					dpm.log.Error(err, "Kubelet socket check failed %d times in a row, shutting down", socketCheckFailures)
					dpm.stopPlugins(pluginMap)
					return errors.New("Failed to access kubelet socket, shutting down")
				}
			}

		case s := <-signalCh:
			switch s {
			case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
				dpm.log.V(3).Info("Received signal \"%v\", shutting down", s)
				dpm.stopPlugins(pluginMap)
				break HandleSignals
			}

		case <-dpm.ctx.Done():
			dpm.log.V(3).Info("Received context done signal, shutting down")
			dpm.stopPlugins(pluginMap)
			break HandleSignals
		}
	}
	return nil
}

func (dpm *Manager) handleNewPlugins(currentPluginsMap map[string]devicePlugin, newPluginsList PluginNameList) {
	var wg sync.WaitGroup
	var pluginMapMutex = &sync.Mutex{}

	// This map is used for faster searches when removing old plugins
	newPluginsSet := make(map[string]bool)

	// Add new plugins
	for _, newPluginLastName := range newPluginsList {
		newPluginsSet[newPluginLastName] = true
		wg.Add(1)
		go func(name string) {
			if _, ok := currentPluginsMap[name]; !ok {
				// add new plugin only if it doesn't already exist
				dpm.log.V(3).Info("Adding a new plugin \"%s\"", name)
				plugin := newDevicePlugin(dpm.lister.GetResourceNamespace(), name, dpm.lister.NewPlugin(name))
				dpm.startPlugin(name, plugin)
				pluginMapMutex.Lock()
				currentPluginsMap[name] = plugin
				pluginMapMutex.Unlock()
			}
			wg.Done()
		}(newPluginLastName)
	}
	wg.Wait()

	// Remove old plugins
	for pluginLastName, currentPlugin := range currentPluginsMap {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			if _, found := newPluginsSet[name]; !found {
				dpm.log.V(3).Info("Remove unused plugin \"%s\"", name)
				dpm.stopPlugin(name, plugin)
				pluginMapMutex.Lock()
				delete(currentPluginsMap, name)
				pluginMapMutex.Unlock()
			}
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()

	dpm.onceReady.Do(func() {
		select {
		case dpm.readyNotifier <- struct{}{}:
			dpm.log.V(0).Info("Device plugins registered, readiness signal sent")
		default: 
		}
	})
}

func (dpm *Manager) startPluginServers(pluginMap map[string]devicePlugin) {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range pluginMap {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			defer wg.Done()
			dpm.startPluginServer(name, plugin)
		}(pluginLastName, currentPlugin)
	}

	wg.Wait()
}

func (dpm *Manager) stopPluginServers(pluginMap map[string]devicePlugin) {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range pluginMap {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			dpm.stopPluginServer(name, plugin)
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) stopPlugins(pluginMap map[string]devicePlugin) {
	var wg sync.WaitGroup
	var pluginMapMutex = &sync.Mutex{}

	for pluginLastName, currentPlugin := range pluginMap {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			dpm.stopPlugin(name, plugin)
			pluginMapMutex.Lock()
			delete(pluginMap, name)
			pluginMapMutex.Unlock()
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) startPlugin(pluginLastName string, plugin devicePlugin) {
	var err error
	if devicePluginImpl, ok := plugin.DevicePluginImpl.(PluginInterfaceStart); ok {
		err = devicePluginImpl.Start()
		if err != nil {
			dpm.log.Error(err, "Failed to start plugin \"%s\": %s", pluginLastName, err)
		}
	}
	if err == nil {
		dpm.startPluginServer(pluginLastName, plugin)
	}
}

func (dpm *Manager) stopPlugin(pluginLastName string, plugin devicePlugin) {
	dpm.stopPluginServer(pluginLastName, plugin)
	if devicePluginImpl, ok := plugin.DevicePluginImpl.(PluginInterfaceStop); ok {
		err := devicePluginImpl.Stop()
		if err != nil {
			dpm.log.Error(err, "Failed to stop plugin \"%s\": %s", pluginLastName, err)
		}
	}
}

func (dpm *Manager) startPluginServer(pluginLastName string, plugin devicePlugin) {
	for i := 1; i <= startPluginServerRetries; i++ {
		err := plugin.StartServer(dpm.ctx)
		if err == nil {
			return
		} else if i == startPluginServerRetries {
			dpm.log.V(3).Info("Failed to start plugin's \"%s\" server, within given %d tries: %s",
				pluginLastName, startPluginServerRetries, err)
		} else {
			dpm.log.Error(err, "Failed to start plugin's \"%s\" server, attempt %d out of %d waiting %d before next try: %s",
				pluginLastName, i, startPluginServerRetries, startPluginServerRetryWait, err)
			time.Sleep(startPluginServerRetryWait)
		}
	}
}

func (dpm *Manager) stopPluginServer(pluginLastName string, plugin devicePlugin) {
	err := plugin.StopServer()
	if err != nil {
		dpm.log.Error(err, "Failed to stop plugin's \"%s\" server", pluginLastName)
	}
}
