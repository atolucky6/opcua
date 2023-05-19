package server

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/afs/server/pkg/opcua/ua"
	"github.com/google/uuid"
)

/*
	 JsonProject is the wrapper root node of the project
	    - Create an instance of JsonProject by using NewEmptyJsonProject() or NewDefaultJsonProject() or NewJsonProjectFromFile() or NewJsonProjectFromBytes()
		- JsonProject will be use to save the OPC UA project
*/
type JsonProject struct {
	// Root is the root node of the project
	Root *JsonObjectNode `json:"root"`
}

// NewEmptyJsonProject returns an JsonProject instance without root node
func NewEmptyJsonProject() *JsonProject {
	return &JsonProject{}
}

// NewDefaultJsonProject returns an JsonProject instance with root node category nodes
func NewDefaultJsonProject(ctx context.Context) *JsonProject {
	p := &JsonProject{}
	p.Root = NewJsonObjectNode(NewRootNode(ctx, true), true)
	return p
}

// NewRootNode returns an Node instance which is define as root of project
func NewRootNode(ctx context.Context, includeCategory bool) *ObjectNode {

	// create root node
	root := NewDefaultObjectNode(
		nil,
		ua.NewQualifiedName(DefaultNameSpace, NodeTypeRoot.String()),
		ua.NewLocalizedText(NodeTypeRoot.String(), DefaultLocale),
		ua.NewLocalizedText(NodeTypeRoot.Description(), DefaultLocale),
		ua.NewDataValue(NodeTypeRoot.Int(), ua.Good, time.Now(), 0, time.Now(), 0),
		ua.NewDataValue(PluginIDCore, ua.Good, time.Now(), 0, time.Now(), 0),
		ua.NewDataValue(uuid.MustParse("a653499a-9c4d-431d-840a-eb78dac3fd88"), ua.Good, time.Now(), 0, time.Now(), 0),
		ctx,
	)

	if includeCategory {
		// add Connectivity node
		categoryConnectivity := NewDefaultObjectNode(
			root,
			ua.NewQualifiedName(DefaultNameSpace, NodeTypeCategoryConnectivity.String()),
			ua.NewLocalizedText(NodeTypeCategoryConnectivity.String(), DefaultLocale),
			ua.NewLocalizedText(NodeTypeCategoryConnectivity.Description(), DefaultLocale),
			ua.NewDataValue(NodeTypeCategoryConnectivity.Int(), ua.Good, time.Now(), 0, time.Now(), 0),
			ua.NewDataValue(PluginIDCore, ua.Good, time.Now(), 0, time.Now(), 0),
			ua.NewDataValue(uuid.MustParse("25ddc197-7dae-4604-9e13-bdad5576a581"), ua.Good, time.Now(), 0, time.Now(), 0),
			ctx,
		)
		root.AddChild(categoryConnectivity)

		// add DataLogger node
		categoryDataLoggers := NewDefaultObjectNode(
			root,
			ua.NewQualifiedName(DefaultNameSpace, NodeTypeCategoryDataLogger.String()),
			ua.NewLocalizedText(NodeTypeCategoryDataLogger.String(), DefaultLocale),
			ua.NewLocalizedText(NodeTypeCategoryDataLogger.Description(), DefaultLocale),
			ua.NewDataValue(NodeTypeCategoryDataLogger.Int(), ua.Good, time.Now(), 0, time.Now(), 0),
			ua.NewDataValue(PluginIDCore, ua.Good, time.Now(), 0, time.Now(), 0),
			ua.NewDataValue(uuid.MustParse("795691c4-f369-4b1a-898c-93c3a6326e4f"), ua.Good, time.Now(), 0, time.Now(), 0),
			ctx,
		)
		root.AddChild(categoryDataLoggers)

		// add Alarms node
		categoryAlarms := NewDefaultObjectNode(
			root,
			ua.NewQualifiedName(DefaultNameSpace, NodeTypeCategoryAlarms.String()),
			ua.NewLocalizedText(NodeTypeCategoryAlarms.String(), DefaultLocale),
			ua.NewLocalizedText(NodeTypeCategoryAlarms.Description(), DefaultLocale),
			ua.NewDataValue(NodeTypeCategoryAlarms.Int(), ua.Good, time.Now(), 0, time.Now(), 0),
			ua.NewDataValue(PluginIDCore, ua.Good, time.Now(), 0, time.Now(), 0),
			ua.NewDataValue(uuid.MustParse("9d0f9ace-0fbf-4606-aba4-997ceb1fb500"), ua.Good, time.Now(), 0, time.Now(), 0),
			ctx,
		)
		root.AddChild(categoryAlarms)
	}
	return root
}

// NewJsonProjectFromFile returns an JsonProject instance by reading json content from provide filePath and convert it to JsonProject
func NewJsonProjectFromFile(filePath string) (*JsonProject, error) {
	jsonFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()
	jsonBytes, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}

	var project JsonProject
	err = json.Unmarshal(jsonBytes, &project)
	if err != nil {
		return nil, err
	}

	return &project, nil
}

// NewJsonProjectFromBytes returns an JsonProject instance by convert the provide json data to JsonProject
func NewJsonProjectFromBytes(data []byte) (*JsonProject, error) {
	project := NewEmptyJsonProject()
	err := json.Unmarshal(data, &project)
	if err != nil {
		return nil, err
	}
	return project, nil
}

// SaveAs save the JsonProject to the specified filePath
func (p *JsonProject) SaveAs(filePath string) error {
	jsonBytes, err := json.MarshalIndent(p, "", "\t")
	if err != nil {
		return err
	}

	err = os.WriteFile(filePath, jsonBytes, 0644)
	if err != nil {
		return err
	}
	return nil
}

// Validate to check whether project is valid or not
func (p *JsonProject) Validate(ctx context.Context) (*ObjectNode, error) {
	if p.Root == nil {
		return nil, ErrRootNodeNotFound
	}

	if len(p.Root.Childs) < 1 {
		return nil, ErrInvalidRootNode
	}

	rootNode, err := p.Root.ToObjectNode(ctx, nil)
	if err != nil {
		return nil, err
	}

	if rootNode.childs == nil || rootNode.childs.Size() < 1 {
		return nil, err
	}

	return rootNode, nil
}
