"""Bounded user inputs for database-backed dynamic groups."""
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class DynamicGroupDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(default="", max_length=2000)
    folder: str = Field(default="", max_length=100)
    color: str = Field(default="#409eff", pattern=r"^#[0-9a-fA-F]{6}$")
    pinned: bool = False
    enabled: bool = True
    universe: Literal["all_a", "limit_up"] = "all_a"
    membership_mode: Literal["retain", "dynamic"] = "retain"
    match_mode: Literal["all", "any"] = "all"
    rules: list[dict[str, Any]] = Field(min_length=1, max_length=30)

    @field_validator("rules")
    @classmethod
    def check_rules(cls, value):
        from app.services.dynamic_group_rules import validate_rules
        return validate_rules(value)


class DynamicGroupUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str | None = Field(default=None, min_length=1, max_length=100)
    description: str | None = Field(default=None, max_length=2000)
    folder: str | None = Field(default=None, max_length=100)
    color: str | None = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")
    pinned: bool | None = None
    enabled: bool | None = None
    universe: Literal["all_a", "limit_up"] | None = None
    membership_mode: Literal["retain", "dynamic"] | None = None
    match_mode: Literal["all", "any"] | None = None
    rules: list[dict[str, Any]] | None = Field(default=None, min_length=1, max_length=30)

    @model_validator(mode="before")
    @classmethod
    def reject_null(cls, value):
        if isinstance(value, dict) and any(item is None for item in value.values()):
            raise ValueError("更新字段不能为 null")
        return value

    @field_validator("rules")
    @classmethod
    def check_rules(cls, value):
        from app.services.dynamic_group_rules import validate_rules
        return validate_rules(value)
