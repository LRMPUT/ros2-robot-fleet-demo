import httpx
import logging
from app.config import settings

logger = logging.getLogger("uvicorn.info")

class KsqlDBClient:
    def __init__(self):
        # ksqlDB REST API url
        self.url = getattr(settings, "KSQLDB_URL", "http://ksqldb-server:8088")

    async def execute_statement(self, ksql_string: str) -> bool:
        """Send a DDL or DML statement to ksqlDB"""
        payload = {
            "ksql": ksql_string,
            "streamsProperties": {}
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.url}/ksql",
                    json=payload,
                    headers={"Content-Type": "application/vnd.ksql.v1+json"},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    logger.info(f"[ksqlDB] statement executed: {ksql_string.strip()[:60]}...")
                    return True
                else:
                    logger.error(f"[ksqlDB] error {response.status_code}: {response.text}")
                    return False
        except Exception as e:
            logger.error(f"[ksqlDB] connection failed: {e}")
            return False

ksqldb_client = KsqlDBClient()
def get_ksqldb_client():
    return ksqldb_client