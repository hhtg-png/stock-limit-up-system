import asyncio
import websockets

async def test():
    print("Connecting to ws://localhost:8000/ws/continuous...")
    async with websockets.connect('ws://localhost:8000/ws/continuous') as ws:
        msg = await ws.recv()
        print('Received:', msg[:500])

asyncio.run(test())
