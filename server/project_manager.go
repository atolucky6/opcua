package server

import (
	"context"
	"os"
	"sync"

	"github.com/afs/server/config"
	"github.com/afs/server/pkg/opcua/ua"
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/google/uuid"
	"github.com/qmuntal/stateless"
	log "github.com/sirupsen/logrus"
)

// ProjectManagerState is the state of the ProjectManager instance
type ProjectManagerState int

// String return the represent ProjectManagerState as string
func (p ProjectManagerState) String() string {
	switch p {
	case PROJECT_STATE_LOADED:
		return "Loaded"
	case PROJECT_STATE_UNLOADED:
		return "Unloaded"
	case PROJECT_STATE_ERROR:
		return "Error"
	case PROJECT_STATE_LOADING:
		return "Loading"
	case PROJECT_STATE_RELOAD:
		return "Reload"
	default:
		return "Unknown"
	}
}

// States of ProjectManagerState
const (
	PROJECT_STATE_ERROR    ProjectManagerState = 4
	PROJECT_STATE_RELOAD   ProjectManagerState = 3
	PROJECT_STATE_LOADED   ProjectManagerState = 2
	PROJECT_STATE_LOADING  ProjectManagerState = 1
	PROJECT_STATE_UNLOADED ProjectManagerState = 0

	triggerLoadProject   string = "Load project"
	triggerErrorOccured  string = "Error occured"
	triggerLoadSuccess   string = "Load success"
	triggerReloadProject string = "Reload project"
	triggerLoadPlugins   string = "Load plugins"
	triggerUnloadPlugins string = "Unload plugins"
	triggerReloadPlugins string = "Reload plugins"
)

/*
ProjectManager is the instance, it will support load and unload the project, provide manage function of Node
  - Create an instance of ProjectManager, by using NewProjectManager()
*/
type ProjectManager struct {
	sync.RWMutex

	// ctx is the context of application,
	// it contains all of the manager instance or some useful information
	ctx context.Context

	// rootNode is an root node of project was loaded into *ProjectManager
	rootNode *ObjectNode

	// entryNodes is the list of entry node (entry node like the main function of each application which in this case is the plugin)
	entryNodes *arraylist.List

	// nodeIdToNodeMapper is an hashmap it will map NodeID to the associated Node (use hasmap for the quick access)
	nodeIdToNodeMapper map[ua.NodeID]*ObjectNode

	// internalIdToNodeMapper is an hashmap it will map InternalId to the associated Node (use hasmap for the quick access)
	internalIdToNodeMapper map[uuid.UUID]*ObjectNode

	// namespaceManager manages the namespaces for a server.
	namespaceManager *NamespaceManager

	// config is the common application config
	config *config.Config

	// currentError is the last error of project manager
	currentError error

	// state is the object that manage the workflows of this *ProjectManager
	state *stateless.StateMachine
}

// NewProjectManager returns new instance of ProjectManager
func NewProjectManager() *ProjectManager {
	if err := os.MkdirAll("./projects/runtime", os.ModeDir|0755); err != nil {
		log.Fatalf("create project directory failed: %s", err)
	}

	return &ProjectManager{
		entryNodes:             arraylist.New(),
		nodeIdToNodeMapper:     map[ua.NodeID]*ObjectNode{},
		internalIdToNodeMapper: map[uuid.UUID]*ObjectNode{},
	}
}

// SetContext set the application context of this project manager
func (p *ProjectManager) SetContext(ctx context.Context) {
	if p.ctx != nil {
		return
	}

	p.ctx = ctx
	p.namespaceManager = ctx.Value(CtxKeyNamespaceManager).(*NamespaceManager)
	p.config = ctx.Value(CtxKeyConfig).(*config.Config)

	p.state = stateless.NewStateMachine(PROJECT_STATE_UNLOADED)
	p.state.Configure(PROJECT_STATE_UNLOADED).
		Permit(triggerLoadProject, PROJECT_STATE_LOADING)

	p.state.Configure(PROJECT_STATE_LOADING).
		OnEntry(p.onLoading).
		Permit(triggerErrorOccured, PROJECT_STATE_ERROR).
		Permit(triggerLoadSuccess, PROJECT_STATE_LOADED)

	p.state.Configure(PROJECT_STATE_LOADED).
		OnEntry(p.onLoaded).
		InternalTransition(triggerReloadPlugins, p.onReloadPlugins).
		InternalTransition(triggerLoadPlugins, p.onLoadPlugins).
		InternalTransition(triggerUnloadPlugins, p.onUnloadPlugins).
		Permit(triggerReloadProject, PROJECT_STATE_RELOAD)

	p.state.Configure(PROJECT_STATE_ERROR).
		OnEntry(p.onError).
		Permit(triggerReloadProject, PROJECT_STATE_RELOAD)

	p.state.Configure(PROJECT_STATE_RELOAD).
		OnEntry(p.onReload).
		Permit(triggerLoadProject, PROJECT_STATE_LOADING)

	p.state.Activate()
}

// Root returns an root node of current loaded project
func (p *ProjectManager) Root() *ObjectNode {
	p.Lock()
	defer p.Unlock()
	return p.rootNode
}

// EntryNodes returns all entries node of current loaded project
func (p *ProjectManager) EntryNodes() []interface{} {
	p.Lock()
	defer p.Unlock()
	return p.entryNodes.Values()
}

// Load the project from file and run it
func (p *ProjectManager) Load() {
	p.Lock()
	defer p.Unlock()
	err := p.state.Fire(triggerLoadProject)
	if err != nil {
		p.state.Fire(triggerErrorOccured, err)
		return
	}
	p.state.Fire(triggerLoadSuccess)
}

// Unload stop the current running project and clean all nodes was stored in *NamespaceManager and *ProjectManagers
func (p *ProjectManager) Unload() {
	p.Lock()
	defer p.Unlock()
	p.state.Deactivate()
	ctx := context.Background()
	p.onUnloadPlugins(ctx)
	p.cleanup()
}

// ReloadProject stop the current running project and load it again
func (p *ProjectManager) ReloadProject() error {
	p.Lock()
	defer p.Unlock()
	if ok, err := p.state.CanFire(triggerReloadProject); !ok {
		return err
	}
	err := p.state.Fire(triggerReloadProject)
	if err != nil {
		p.state.Fire(triggerErrorOccured, err)
		return err
	}
	err = p.state.Fire(triggerLoadProject)
	if err != nil {
		p.state.Fire(triggerErrorOccured, err)
		return err
	}
	p.state.Fire(triggerLoadSuccess)
	return nil
}

// ReloadPlugins
func (p *ProjectManager) ReloadPlugins() error {
	p.Lock()
	defer p.Unlock()
	err := p.checkState()
	if err != nil {
		return err
	}
	if ok, err := p.state.CanFire(triggerReloadPlugins); !ok {
		return err
	}
	err = p.state.Fire(triggerReloadPlugins)
	return err
}

// UnloadPlugins
func (p *ProjectManager) UnloadPlugins() error {
	p.Lock()
	defer p.Unlock()
	err := p.checkState()
	if err != nil {
		return err
	}
	if ok, err := p.state.CanFire(triggerUnloadPlugins); !ok {
		return err
	}
	err = p.state.Fire(triggerUnloadPlugins)
	return err
}

// LoadPlugins
func (p *ProjectManager) LoadPlugins() error {
	p.Lock()
	defer p.Unlock()
	err := p.checkState()
	if err != nil {
		return err
	}
	if ok, err := p.state.CanFire(triggerLoadPlugins); !ok {
		return err
	}
	err = p.state.Fire(triggerLoadPlugins)
	return err
}

// GetCurrentError returns an current error of this *ProjectManager
func (p *ProjectManager) GetCurrentError() error {
	p.Lock()
	defer p.Unlock()
	return p.currentError
}

// GetCurrentState returns an current state of this *ProjectManager
func (p *ProjectManager) GetCurrentState() ProjectManagerState {
	p.Lock()
	defer p.Unlock()
	return p.state.MustState().(ProjectManagerState)
}

// HasError returns an error if this ProjectManager has error
func (p *ProjectManager) HasError() error {
	p.Lock()
	defer p.Unlock()

	switch p.state.MustState().(ProjectManagerState) {
	case PROJECT_STATE_ERROR:
		return p.currentError
	case PROJECT_STATE_UNLOADED:
		return ErrProjectNotLoaded
	}
	return nil
}

// AddNode will add an node into a namespace manager
func (p *ProjectManager) AddNode(parent, node *ObjectNode) error {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return err
	}

	// assign plugin props for node before add
	node.AssignPluginProps()

	// add child node to parent
	err = parent.AddChild(node)
	if err != nil {
		return err
	}

	// if add success then add node to namespace manager
	err = p.namespaceManager.AddNode(node)
	if err != nil {
		parent.RemoveChild(node)
		return err
	}

	// add all of the node properties into namespace manager
	for _, prop := range node.properties {
		p.namespaceManager.AddNode(prop)
	}

	// cache the node
	p.nodeIdToNodeMapper[node.GetNodeID()] = node
	p.internalIdToNodeMapper[node.MustGetProperty(PropertyNameInternalId).GetValue().Value.(uuid.UUID)] = node

	// if node is an entry node then start it
	if node.IsEntry() {
		p.entryNodes.Add(node)
		go node.GetPlugin().Start(node)
	}

	return nil
}

// RemoveNode will remove an node out of namespace manager
func (p *ProjectManager) RemoveNode(node *ObjectNode) error {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return err
	}

	if node.parent == nil {
		return ErrParentNotFound
	}

	err = node.parent.RemoveChild(node)
	if err != nil {
		return err
	}

	// loop into child and it self node
	// to check if any node is entry node then stop it
	node.ForEachSelfDepth(func(child *ObjectNode) {
		if child.IsEntry() {
			p.entryNodes.Remove(p.entryNodes.IndexOf(child))
			go node.GetPlugin().Stop(node)
		}
	})

	// remove a node out of namespace manager
	err = p.namespaceManager.DeleteNode(node, true)
	if err != nil {
		return err
	}
	return nil
}

// Save will save the current loaded project to the config.RuntimeProjectFile path
func (p *ProjectManager) Save() error {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return err
	}

	// convert root node to json node
	jsonNode := NewJsonObjectNode(p.rootNode, true)
	// create empty project to store the root node
	project := NewEmptyJsonProject()
	project.Root = jsonNode

	// save to runtime project file
	err = project.SaveAs(p.config.App.ProjectPath)
	if err != nil {
		return err
	}
	return nil
}

// GetProject returns the current loaded project
func (p *ProjectManager) GetProject() (*JsonProject, error) {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return nil, err
	}

	// convert root node to json node
	jsonNode := NewJsonObjectNode(p.rootNode, true)
	// create empty project to store the root node
	project := NewEmptyJsonProject()
	project.Root = jsonNode
	return project, nil
}

// GetAllNodes return all nodes object in loaded project
func (p *ProjectManager) GetAllNodes(includeRoot bool) ([]*ObjectNode, error) {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return nil, err
	}

	nodes := []*ObjectNode{}
	if includeRoot {
		p.rootNode.ForEachSelfDepth(func(child *ObjectNode) {
			nodes = append(nodes, child)
		})
	} else {
		p.rootNode.ForEachDepth(func(child *ObjectNode) {
			nodes = append(nodes, child)
		})
	}
	return nodes, nil
}

// GetNodeByNodeId returns the node in project by it NodeId
func (p *ProjectManager) GetNodeByNodeId(nodeId ua.NodeID) (*ObjectNode, error) {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return nil, err
	}

	node, found := p.nodeIdToNodeMapper[nodeId]
	if !found {
		return nil, ErrNotFound
	}
	return node, nil
}

// GetNodeByInternalID returns the node in project by it InternalId
func (p *ProjectManager) GetNodeByInternalId(internalId uuid.UUID) (*ObjectNode, error) {
	p.Lock()
	defer p.Unlock()

	err := p.checkState()
	if err != nil {
		return nil, err
	}

	node, found := p.internalIdToNodeMapper[internalId]
	if !found {
		return nil, ErrNotFound
	}
	return node, nil
}

// GetNode returns the node in project by it NodeId or InternalId
func (p *ProjectManager) GetNode(id string) (*ObjectNode, error) {
	internalId, err := uuid.Parse(id)
	if err == nil {
		return p.GetNodeByInternalId(internalId)
	}
	return p.GetNodeByNodeId(ua.ParseNodeIDString(id))
}

func (p *ProjectManager) ReplaceNodeID(oldId, newId ua.NodeID) {
	p.Lock()
	defer p.Unlock()
	if node, found := p.nodeIdToNodeMapper[oldId]; found {
		delete(p.nodeIdToNodeMapper, oldId)
		p.nodeIdToNodeMapper[newId] = node
	}
}

// onLoading handler of state PROJECT_STATE_LOADING
func (p *ProjectManager) onLoading(ctx context.Context, args ...interface{}) error {
	// check the runtime project file is existed
	// if not create a new one
	if _, err := os.Stat(p.config.App.ProjectPath); os.IsNotExist(err) {
		NewDefaultJsonProject(p.ctx).SaveAs(p.config.App.ProjectPath)

	}

	// load project from runtime project file path
	project, err := NewJsonProjectFromFile(p.config.App.ProjectPath)
	if err != nil {
		return err
	}

	// validate the project
	rootNode, err := project.Validate(p.ctx)
	if err != nil {
		return err
	}

	p.cleanup()
	// assign new root node
	p.rootNode = rootNode

	// cache all child node from loaded root node
	rootNode.ForEachSelfDepth(func(child *ObjectNode) {
		if child.IsEntry() {
			p.entryNodes.Add(child)
		}
		p.nodeIdToNodeMapper[child.GetNodeID()] = child
		p.internalIdToNodeMapper[child.MustGetProperty(PropertyNameInternalId).GetValue().Value.(uuid.UUID)] = child
		child.AssignPluginProps()
	})

	// add all nodes include properties to namespace manager
	p.rootNode.ForEachSelfDepth(func(child *ObjectNode) {
		p.namespaceManager.AddNode(child)
		for _, propNode := range child.GetProperties() {
			p.namespaceManager.AddNode(propNode)
		}
	})

	p.onLoadPlugins(ctx, args)
	return nil
}

// onLoading handler of state PROJECT_STATE_LOAD_PLUGINS
func (p *ProjectManager) onLoadPlugins(ctx context.Context, args ...interface{}) error {
	// start nodes that was marked entry = true
	for _, item := range p.entryNodes.Values() {
		go item.(*ObjectNode).GetPlugin().Start(item.(*ObjectNode))
	}
	return nil
}

// onLoading handler of state PROJECT_STATE_LOADED
func (p *ProjectManager) onLoaded(ctx context.Context, args ...interface{}) error {
	log.Traceln("*ProjectManager << onLoaded")
	return nil
}

// onLoading handler of state PROJECT_STATE_RELOAD
func (p *ProjectManager) onReload(ctx context.Context, args ...interface{}) error {
	log.Traceln("*ProjectManager << onReload")
	p.onUnloadPlugins(ctx, args)
	return nil
}

// onLoading handler of state PROJECT_STATE_UNLOAD_PLUGINS
func (p *ProjectManager) onUnloadPlugins(ctx context.Context, args ...interface{}) error {
	log.Traceln("*ProjectManager << onUnloadPlugins")
	// stop nodes that was marked entry = true
	for _, item := range p.entryNodes.Values() {
		go item.(*ObjectNode).GetPlugin().Stop(item.(*ObjectNode))
	}
	return nil
}

// onLoading handler of state PROJECT_STATE_ERROR
func (p *ProjectManager) onError(ctx context.Context, args ...interface{}) error {
	log.Traceln("*ProjectManager << onError")
	p.currentError = args[0].(error)
	return nil
}

// onLoading handler of state PROJECT_STATE_RELOAD_PLUGINS
func (p *ProjectManager) onReloadPlugins(ctx context.Context, args ...interface{}) error {
	log.Traceln("*ProjectManager << onReloadPlugins")
	p.onUnloadPlugins(ctx, args...)
	p.onLoadPlugins(ctx, args...)
	return nil
}

// cleanup clear all nodes was stored in this *ProjectManager and *NamespaceManager
func (p *ProjectManager) cleanup() {
	log.Traceln("*ProjectManager << cleanup")
	p.entryNodes.Clear()
	for key := range p.nodeIdToNodeMapper {
		delete(p.nodeIdToNodeMapper, key)
	}
	for key := range p.internalIdToNodeMapper {
		delete(p.internalIdToNodeMapper, key)
	}
	if p.rootNode != nil {
		p.namespaceManager.DeleteNode(p.rootNode, true)
	}
	p.rootNode = nil
}

// checkState to make sure this *ProjectManager was in valid state to
func (p *ProjectManager) checkState() error {
	state := p.state.MustState()
	if state == PROJECT_STATE_ERROR {
		return p.currentError
	}
	return nil
}
