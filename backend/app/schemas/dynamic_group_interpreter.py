"""Public contract for interpretation only; no group writes occur here."""
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.schemas.dynamic_group import DynamicGroupDefinition


ShortText = Annotated[str, Field(min_length=1, max_length=2000)]


class DynamicGroupInterpretRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    prompt: str = Field(min_length=1, max_length=2000)
    current_definition: DynamicGroupDefinition | None = None


class DynamicGroupInterpretResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    status: Literal["ready", "needs_clarification", "unsupported", "error"]
    message: str = Field(min_length=1, max_length=2000)
    definition: DynamicGroupDefinition | None = None
    assumptions: list[ShortText] = Field(default_factory=list, max_length=30)
    questions: list[ShortText] = Field(default_factory=list, max_length=30)
    unsupported: list[ShortText] = Field(default_factory=list, max_length=30)
    model: str | None = Field(default=None, max_length=200)

    @model_validator(mode="after")
    def executable_only_when_ready(self):
        if self.status == "ready":
            if self.definition is None or self.questions or self.unsupported:
                raise ValueError("只有完整且通过验证的方案可以执行")
        elif self.definition is not None:
            raise ValueError("尚未确认的解释不能包含可执行定义")
        return self
