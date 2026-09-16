package http

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/example/golang-staterpack/internal/service"
)

// TaskHandler exposes the task use cases over HTTP.
type TaskHandler struct {
	svc *service.TaskService
}

// NewTaskHandler wires the service.
func NewTaskHandler(svc *service.TaskService) *TaskHandler {
	return &TaskHandler{svc: svc}
}

// Create handles POST /api/v1/tasks.
func (h *TaskHandler) Create(c *fiber.Ctx) error {
	var in service.CreateTaskInput
	if err := c.BodyParser(&in); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "malformed JSON body")
	}

	task, err := h.svc.Create(c.Context(), in)
	if err != nil {
		return err // mapped by ErrorHandler
	}
	return c.Status(fiber.StatusCreated).JSON(task)
}

// Get handles GET /api/v1/tasks/:id.
func (h *TaskHandler) Get(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid task id")
	}

	task, err := h.svc.Get(c.Context(), id)
	if err != nil {
		return err
	}
	return c.JSON(task)
}

// List handles GET /api/v1/tasks.
func (h *TaskHandler) List(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 20)
	offset := c.QueryInt("offset", 0)

	tasks, total, err := h.svc.List(c.Context(), limit, offset)
	if err != nil {
		return err
	}

	return c.JSON(fiber.Map{
		"data":   tasks,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// Delete handles DELETE /api/v1/tasks/:id.
func (h *TaskHandler) Delete(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid task id")
	}

	if err := h.svc.Delete(c.Context(), id); err != nil {
		return err
	}
	return c.SendStatus(fiber.StatusNoContent)
}
