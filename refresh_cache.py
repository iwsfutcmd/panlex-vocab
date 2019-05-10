import asyncio
import panlex_db

async def run():
    await panlex_db.connect()
    await panlex_db.refresh_cache()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
