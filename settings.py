from pydantic import BaseModel


class VerticaConn(BaseModel):
    host: str
    port: int = 5433
    user: str
    password: str
    database: str


class Settings(BaseModel):
    left: VerticaConn = VerticaConn(
        host="LEFT_VERTICA_HOST",
        user="LEFT_USER",
        password="LEFT_PASSWORD",
        database="LEFT_DB",
    )
    right: VerticaConn = VerticaConn(
        host="RIGHT_VERTICA_HOST",
        user="RIGHT_USER",
        password="RIGHT_PASSWORD",
        database="RIGHT_DB",
    )
    mapping_file: str = "mappings/workday_mapping_sample.xlsx"
    god_mode_default: bool = False


settings = Settings()
