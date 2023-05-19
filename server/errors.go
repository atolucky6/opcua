package server

import (
	"github.com/afs/server/pkg/eris"
	"github.com/afs/server/pkg/msg"
)

var (
	ErrNotFound            = eris.New(msg.NotFound)
	ErrInvalidValue        = eris.New(msg.InvalidValue)
	ErrFieldRequired       = eris.New(msg.FieldRequired)
	ErrValueOutOfRange     = eris.New(msg.ValueIsOutOfRange)
	ErrRootNodeNotFound    = eris.New(msg.RootNodeNotFound)
	ErrInvalidRootNode     = eris.New(msg.InvalidRootNode)
	ErrInvalidNodeType     = eris.New(msg.InvalidNodeType)
	ErrFieldExisted        = eris.New(msg.FieldExisted)
	ErrInvalidField        = eris.New(msg.InvalidField)
	ErrNodeTypeNotAccepted = eris.New(msg.NodeTypeNotAccepted)
	ErrInvalidIndex        = eris.New(msg.InvalidIndex)
	ErrInvalidFormType     = eris.New(msg.InvalidFormType)
	ErrParentNotFound      = eris.New(msg.ParentNotFound)
	ErrProjectNotLoaded    = eris.New("Project was not ready yet")
)
