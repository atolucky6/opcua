package server

type Response struct {
	Success     bool    `json:"success,omitempty"`
	Message     string  `json:"message,omitempty"`
	Error       error   `json:"error,omitempty"`
	FieldErrors []error `json:"fieldErrors,omitempty"`
}

func NewErrorResponse(message string, err error) *Response {
	return &Response{
		Success: false,
		Message: message,
		Error:   err,
	}
}

func NewSuccessResponse(message string) *Response {
	return &Response{
		Success: true,
		Message: message,
	}
}

func NewFieldErrorsResponse(message string, fieldErrors ...error) *Response {
	return &Response{
		Success:     false,
		FieldErrors: fieldErrors,
	}
}
