from typing import Dict, List, Optional, Union
import threading
import logging
import asyncio
import hashlib
import pickle
import base64
import time
import uuid
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._cache: Dict[str, Dict] = {}
        self._events = []
        self._initialized = getattr(self, '_initialized', False)

        if not self._initialized:
            self._expiry_times: Dict[str, float] = {}
            self._event_handlers = {}
            self._initialized = True

            # Start the background thread for periodic cleanup
            self._cleanup_thread = threading.Thread(target=self._periodic_cleanup, daemon=True)
            self._cleanup_thread.start()

    async def get_or_compute(self, key: str, compute_func, ttl: int = 3600) -> Optional[Dict]:
        current_time = time.time()
        cache_entry = self._cache.get(key)

        if cache_entry and key in self._expiry_times:
            if current_time < self._expiry_times[key]:
                return pickle.loads(base64.b64decode(cache_entry['data']))

        try:
            result = await compute_func()
            encoded_data = base64.b64encode(pickle.dumps(result))
            self._cache[key] = {
                'data': encoded_data,
                'metadata': {
                    'created_at': current_time,
                    'ttl': ttl,
                    'access_count': 0
                }
            }
            self._expiry_times[key] = current_time + ttl
            return result
        except Exception as e:
            logger.error(f"Error computing value for key {key}: {str(e)}")
            return None

    def register_event_handler(self, event_type: str, handler_func) -> str:
        handler_id = str(uuid.uuid4())
        self._event_handlers[handler_id] = {
            'type': event_type,
            'handler': handler_func
        }
        return handler_id

    def process_events(self, max_events: int = 100) -> List[Dict]:
        processed = []
        remaining_events = []

        for event in self._events[:max_events]:
            event_type = event.get('type')
            handlers = [
                handler for handler_id, handler in self._event_handlers.items()
                if handler['type'] == event_type
            ]

            for handler in handlers:
                try:
                    handler['handler'](event)
                    processed.append(event)
                except Exception as e:
                    logger.error(f"Error processing event: {str(e)}")
                    remaining_events.append(event)

        self._events = remaining_events + self._events[max_events:]
        return processed

    def cleanup_expired(self) -> int:
        current_time = time.time()
        expired_keys = [
            key for key, expiry_time in self._expiry_times.items()
            if current_time > expiry_time
        ]

        for key in expired_keys:
            del self._cache[key]
            del self._expiry_times[key]

        return len(expired_keys)

    def _periodic_cleanup(self):
        while True:
            self.cleanup_expired()
            time.sleep(3600)  # Run cleanup every hour

    async def batch_operation(self, operations: List[Dict]) -> Dict[str, Union[bool, str]]:
        results = {}
        for op in operations:
            op_type = op.get('type')
            key = op.get('key')

            if not all([op_type, key]):
                continue

            if op_type == 'delete':
                success = self.delete_entry(key)
                results[key] = success
            elif op_type == 'update':
                data = op.get('data')
                if data:
                    success = await self.update_entry(key, data)
                    results[key] = success
            elif op_type == 'get':
                result = self.get_entry(key)
                results[key] = result if result else ''

        return results

    async def update_entry(self, key: str, data: Dict) -> bool:
        try:
            encoded_data = base64.b64encode(pickle.dumps(data))
            if key in self._cache:
                self._cache[key]['data'] = encoded_data
                self._cache[key]['metadata']['updated_at'] = time.time()
                return True
            return False
        except Exception as e:
            logger.error(f"Error updating entry {key}: {str(e)}")
            return False

    def delete_entry(self, key: str) -> bool:
        try:
            if key in self._cache:
                del self._cache[key]
                if key in self._expiry_times:
                    del self._expiry_times[key]
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting entry {key}: {str(e)}")
            return False

    def get_entry(self, key: str) -> Optional[Dict]:
        try:
            if key in self._cache:
                cache_entry = self._cache[key]
                data = pickle.loads(base64.b64decode(cache_entry['data']))
                cache_entry['metadata']['access_count'] += 1
                return data
            return None
        except Exception as e:
            logger.error(f"Error retrieving entry {key}: {str(e)}")
            return None

    def export_metrics(self) -> Dict:
        return {
            'cache_size': len(self._cache),
            'event_count': len(self._events),
            'handler_count': len(self._event_handlers),
            'expired_keys': len([
                k for k, v in self._expiry_times.items()
                if time.time() > v
            ])
        }

async def main():
    cache_manager = CacheManager()

    async def example_compute():
        await asyncio.sleep(0.1)
        return {'value': 'computed_result'}

    async def run_operations():
        tasks = []
        for i in range(5):
            key = f'test_key_{i}'
            tasks.append(cache_manager.get_or_compute(key, example_compute))

        results = await asyncio.gather(*tasks)

        operations = [
            {'type': 'get', 'key': 'test_key_0'},
            {'type': 'update', 'key': 'test_key_1', 'data': {'value': 'updated'}},
            {'type': 'delete', 'key': 'test_key_2'}
        ]

        batch_results = await cache_manager.batch_operation(operations)
        print(json.dumps(batch_results, indent=2))

        metrics = cache_manager.export_metrics()
        print(json.dumps(metrics, indent=2))

    await run_operations()

if __name__ == "__main__":
    asyncio.run(main())
