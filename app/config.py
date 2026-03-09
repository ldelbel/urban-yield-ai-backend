from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bright_data_token: str = "730af5ea-5de6-4b16-97df-cc5b4e9ef1d2"
    bright_data_api_token: str = "730af5ea-5de6-4b16-97df-cc5b4e9ef1d2"
    bright_data_groups: str = "advanced_scraping,business"
    arcgis_services: str = "https://services7.arcgis.com/xNUwUjOJqYE54USz/ArcGIS/rest/services"
    h3_resolution: int = 8
    claude_api_key: str = ""
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://urban-yield-ai-frontend.pages.dev",
    ]

    @property
    def arcgis_catalog_url(self) -> str:
        return f"{self.arcgis_services}?f=json"

    @property
    def arcgis_permits_url(self) -> str:
        return f"{self.arcgis_services}/Building_Permit_viewlayer/FeatureServer/0/query"

    @property
    def arcgis_business_licenses_url(self) -> str:
        return f"{self.arcgis_services}/Business_view/FeatureServer/0/query"

    @property
    def arcgis_code_enforcement_url(self) -> str:
        return f"{self.arcgis_services}/Code_Enforcement_view/FeatureServer/0/query"

    @property
    def arcgis_311_url(self) -> str:
        return f"{self.arcgis_services}/Received_311_Service_Request/FeatureServer/0/query"

    @property
    def arcgis_zoning_url(self) -> str:
        return f"{self.arcgis_services}/Zoning_HN/FeatureServer/0/query"

    @property
    def arcgis_vacancies_url(self) -> str:
        return f"{self.arcgis_services}/Vacant_Properties/FeatureServer/2/query"

    @property
    def bright_data_sse_url(self) -> str:
        return (
            f"https://mcp.brightdata.com/sse"
            f"?token={self.bright_data_token}"
            f"&groups={self.bright_data_groups}"
        )


settings = Settings()
