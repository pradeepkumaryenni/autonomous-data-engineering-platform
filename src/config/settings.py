from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Offline demo: sample data + fixture workspace, rule-based reasoning, no network calls
    demo_mode: bool = Field(default=False, description="Run against bundled sample data")

    # Databricks
    databricks_host: str = Field(
        default="https://localhost", description="Databricks workspace URL"
    )
    databricks_token: str = Field(default="", description="Databricks PAT token")
    databricks_cluster_id: str = Field(default="", description="Default cluster ID")

    # Azure OpenAI (optional: without a key the agents use their rule-based fallback)
    azure_openai_endpoint: str = Field(
        default="https://localhost", description="Azure OpenAI endpoint"
    )
    azure_openai_api_key: str = Field(default="", description="Azure OpenAI API key")
    azure_openai_deployment: str = Field(default="gpt-4o", description="Deployment name")
    azure_openai_api_version: str = Field(default="2024-08-01-preview", description="API version")

    # App
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Log level")
    agent_timeout_seconds: int = Field(
        default=300, description="Timeout for one full agent-graph run"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def llm_configured(self) -> bool:
        return bool(self.azure_openai_api_key)

    def get_llm(self):
        from langchain_openai import AzureChatOpenAI

        return AzureChatOpenAI(
            azure_endpoint=self.azure_openai_endpoint,
            azure_deployment=self.azure_openai_deployment,
            openai_api_version=self.azure_openai_api_version,
            api_key=self.azure_openai_api_key,
            temperature=0,
            max_tokens=4096,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
