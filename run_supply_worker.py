import asyncio
import os
import logging
from supply_tasks_repo import SupplyTasksRepo
from supply_worker import SupplyWorker

logging.basicConfig(level=logging.INFO)

async def main():
    repo = SupplyTasksRepo()
    worker = SupplyWorker(
        client_id=os.getenv("OZON_CLIENT_ID", ""),
        api_key=os.getenv("OZON_API_KEY", ""),
        repo=repo,
        poll_interval=int(os.getenv("SUPPLY_PROCESS_INTERVAL", "45")),
    )

    # Пример создания тестовой задачи (если нет):
    if not repo.list():
        repo.new_task(
            warehouse_id=int(os.getenv("WAREHOUSE_ID", "1020000000000")),
            dropoff_warehouse_id=int(os.getenv("DROP_OFF_WAREHOUSE_ID", "1020000000000")),
            desired_from_iso=os.getenv("DESIRED_FROM_ISO", "2025-09-26T08:00:00Z"),
            desired_to_iso=os.getenv("DESIRED_TO_ISO", "2025-09-26T09:00:00Z"),
            sku_items=[{"sku": 123456, "proposed_qty": 10}],
            chat_id=0
        )

    await worker.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass