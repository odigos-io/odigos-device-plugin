package dpm

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/golang/glog"

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
	lister ListerInterface
}

// NewManager is the canonical way of initializing Manager. User must provide ListerInterface
// implementation. Lister will provide information about handled resources, monitor their
// availability and provide method to spawn plugins that will handle found resources.
func NewManager(lister ListerInterface) *Manager {
	dpm := &Manager{
		lister: lister,
	}
	return dpm
}

// Run starts the Manager. It sets up the infrastructure and handles system signals, Kubelet socket
// watch and monitoring of available resources as well as starting and stopping of plugins.
func (dpm *Manager) Run() {
	glog.V(3).Info("Starting device plugin manager")

	// First important signal channel is the os signal channel. We only care about (somewhat) small
	// subset of available signals.
	glog.V(3).Info("Registering for system signal notifications")
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)

	// Attempt to initialize filesystem watcher
	glog.V(3).Info("Registering for notifications of filesystem changes in device plugin directory")
	var (
		fsWatcher        *fsnotify.Watcher
		err              error
		usePolling       bool
		socketChangeNotifyCh   chan struct{}
		pollingStopCh      chan struct{}
		fsWatcherEvents  <-chan fsnotify.Event
		pollingEvents <-chan struct{}
	)

	fsWatcher, err = func() (*fsnotify.Watcher, error) {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			glog.Warningf("Failed to create fsnotify watcher: %v, falling back to polling", err)
			return nil, err
		}
		if err := w.Add(pluginapi.DevicePluginPath); err != nil {
			glog.Warningf("Failed to watch device plugin path: %v, falling back to polling", err)
			w.Close()
			return nil, err
		}
		return w, nil
	}()

	defer func() {
		if fsWatcher != nil {
			fsWatcher.Close()
		}
	}()

	if err != nil {
		usePolling = true
		socketChangeNotifyCh = make(chan struct{}, 1) // Buffered channel for socket creation/modification
		pollingStopCh = make(chan struct{})
		go startPolling(pluginapi.KubeletSocket, socketChangeNotifyCh, pollingStopCh)
		pollingEvents = socketChangeNotifyCh
	} else {
		fsWatcherEvents = fsWatcher.Events
	}

	// Start plugin discovery
	var pluginMap = make(map[string]devicePlugin)
	glog.V(3).Info("Starting Discovery on new plugins")
	pluginsCh := make(chan PluginNameList)
	defer close(pluginsCh)
	go dpm.lister.Discover(pluginsCh)

	// Main event loop
	glog.V(3).Info("Handling incoming signals")
HandleSignals:
	for {
		select {
		case newPluginsList := <-pluginsCh:
			glog.V(3).Infof("Received new list of plugins: %s", newPluginsList)
			dpm.handleNewPlugins(pluginMap, newPluginsList)

		case event := <-fsWatcherEvents:
			if event.Name == pluginapi.KubeletSocket {
				glog.V(3).Infof("Received kubelet socket event: %s", event)
				if event.Op&fsnotify.Create == fsnotify.Create {
					dpm.startPluginServers(pluginMap)
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					dpm.stopPluginServers(pluginMap)
				}
			}

		case <-pollingEvents:
			glog.V(3).Infof("Kubelet socket modified or created (polling)")
			dpm.startPluginServers(pluginMap)

		case s := <-signalCh:
			switch s {
			case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
				glog.V(3).Infof("Received signal \"%v\", shutting down", s)
				if usePolling {
					close(pollingStopCh)
				}
				dpm.stopPlugins(pluginMap)
				break HandleSignals
			}
		}
	}
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
				glog.V(3).Infof("Adding a new plugin \"%s\"", name)
				plugin := newDevicePlugin(dpm.lister.GetResourceNamespace(), name, dpm.lister.NewPlugin(name))
				startPlugin(name, plugin)
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
				glog.V(3).Infof("Remove unused plugin \"%s\"", name)
				stopPlugin(name, plugin)
				pluginMapMutex.Lock()
				delete(currentPluginsMap, name)
				pluginMapMutex.Unlock()
			}
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) startPluginServers(pluginMap map[string]devicePlugin) {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range pluginMap {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			startPluginServer(name, plugin)
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) stopPluginServers(pluginMap map[string]devicePlugin) {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range pluginMap {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			stopPluginServer(name, plugin)
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
			stopPlugin(name, plugin)
			pluginMapMutex.Lock()
			delete(pluginMap, name)
			pluginMapMutex.Unlock()
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func startPlugin(pluginLastName string, plugin devicePlugin) {
	var err error
	if devicePluginImpl, ok := plugin.DevicePluginImpl.(PluginInterfaceStart); ok {
		err = devicePluginImpl.Start()
		if err != nil {
			glog.Errorf("Failed to start plugin \"%s\": %s", pluginLastName, err)
		}
	}
	if err == nil {
		startPluginServer(pluginLastName, plugin)
	}
}

func stopPlugin(pluginLastName string, plugin devicePlugin) {
	stopPluginServer(pluginLastName, plugin)
	if devicePluginImpl, ok := plugin.DevicePluginImpl.(PluginInterfaceStop); ok {
		err := devicePluginImpl.Stop()
		if err != nil {
			glog.Errorf("Failed to stop plugin \"%s\": %s", pluginLastName, err)
		}
	}
}

func startPluginServer(pluginLastName string, plugin devicePlugin) {
	for i := 1; i <= startPluginServerRetries; i++ {
		err := plugin.StartServer()
		if err == nil {
			return
		} else if i == startPluginServerRetries {
			glog.V(3).Infof("Failed to start plugin's \"%s\" server, within given %d tries: %s",
				pluginLastName, startPluginServerRetries, err)
		} else {
			glog.Errorf("Failed to start plugin's \"%s\" server, attempt %d out of %d waiting %d before next try: %s",
				pluginLastName, i, startPluginServerRetries, startPluginServerRetryWait, err)
			time.Sleep(startPluginServerRetryWait)
		}
	}
}

func stopPluginServer(pluginLastName string, plugin devicePlugin) {
	err := plugin.StopServer()
	if err != nil {
		glog.Errorf("Failed to stop plugin's \"%s\" server: %s", pluginLastName, err)
	}
}

func startPolling(socketPath string, notifyStart chan struct{}, stop chan struct{}) {
	glog.V(3).Infof("Starting polling for socket: %s", socketPath)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastModTime time.Time
	socketExists := false

	for {
		select {
		case <-ticker.C:
			info, err := os.Stat(socketPath)
			if err == nil {
				// Socket exists
				modTime := info.ModTime()
				if !socketExists || modTime.After(lastModTime) {
					lastModTime = modTime
					socketExists = true
					glog.V(3).Infof("Detected modification or creation of: %s", socketPath)
					select {
					case notifyStart <- struct{}{}:
						glog.V(3).Infof("Sent notifyStart signal")
					default:
					}
				}
			} else {
				// Socket does not exist or other error occurred
				glog.V(3).Infof("os.Stat(%s) error: %v", socketPath, err)
			}

		case <-stop:
			glog.V(3).Info("Stopping polling loop")
			return
		}
	}
}
