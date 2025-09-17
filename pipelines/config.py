from pydantic import BaseModel
from pathlib import Path
import os


class Settings(BaseModel):
    data_dir: Path = Path(os.getenv("DATA_LAKE", "data"))
    years: str = os.getenv("DEFAULT_YEARS", "2015-2024")


settings = Settings()
