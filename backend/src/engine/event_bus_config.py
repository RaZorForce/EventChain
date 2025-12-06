"""
Event bus configuration loader and validator.

Loads event_bus.yaml and provides configuration for the event routing system.
"""
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


@dataclass
class EventConfig:
    """Configuration for a single event type."""
    name: str
    description: str
    provider: str
    consumers: List[str]
    priority: int


@dataclass
class IslandConfig:
    """Configuration for an island."""
    name: str
    produces: List[str]
    consumes: List[str]
    description: str


@dataclass
class EventBusConfig:
    """Complete event bus configuration."""
    events: Dict[str, EventConfig] = field(default_factory=dict)
    islands: Dict[str, IslandConfig] = field(default_factory=dict)
    synchronous_events: List[str] = field(default_factory=list)
    asynchronous_events: List[str] = field(default_factory=list)
    queue_type: str = "priority"
    queue_max_size: int = 10000
    retry_enabled: bool = True
    retry_max_attempts: int = 3
    monitoring_enabled: bool = True
    log_level: str = "INFO"


class EventBusConfigLoader:
    """Loads and validates event bus configuration from YAML."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the config loader.
        
        Args:
            config_path: Path to event_bus.yaml. If None, uses default location.
        """
        if config_path is None:
            # Go up from src/engine/ to backend/config/
            config_path = Path(__file__).parent.parent.parent / "config" / "event_bus.yaml"
        
        self.config_path = config_path
        self._raw_config: Dict[str, Any] = {}
        self.config: Optional[EventBusConfig] = None
    
    def load(self) -> EventBusConfig:
        """Load and parse the event bus configuration."""
        with open(self.config_path, 'r') as f:
            self._raw_config = yaml.safe_load(f)
        
        self.config = self._parse_config()
        self._validate_config()
        return self.config
    
    def _parse_config(self) -> EventBusConfig:
        """Parse raw YAML into EventBusConfig."""
        config = EventBusConfig()
        
        # Parse events
        for event_data in self._raw_config.get('events', []):
            event = EventConfig(
                name=event_data['name'],
                description=event_data['description'],
                provider=event_data['provider'],
                consumers=event_data['consumers'],
                priority=event_data['priority']
            )
            config.events[event.name] = event
        
        # Parse islands
        for island_name, island_data in self._raw_config.get('islands', {}).items():
            island = IslandConfig(
                name=island_name,
                produces=island_data['produces'],
                consumes=island_data['consumes'],
                description=island_data['description']
            )
            config.islands[island_name] = island
        
        # Parse routing
        routing = self._raw_config.get('routing', {})
        config.synchronous_events = routing.get('synchronous_events', [])
        config.asynchronous_events = routing.get('asynchronous_events', [])
        
        queue_config = routing.get('queue', {})
        config.queue_type = queue_config.get('type', 'priority')
        config.queue_max_size = queue_config.get('max_size', 10000)
        
        # Parse handlers
        handlers = self._raw_config.get('handlers', {})
        retry = handlers.get('retry', {})
        config.retry_enabled = retry.get('enabled', True)
        config.retry_max_attempts = retry.get('max_attempts', 3)
        
        # Parse monitoring
        monitoring = self._raw_config.get('monitoring', {})
        metrics = monitoring.get('metrics', {})
        config.monitoring_enabled = metrics.get('enabled', True)
        
        logging = monitoring.get('logging', {})
        config.log_level = logging.get('level', 'INFO')
        
        return config
    
    def _validate_config(self):
        """Validate the configuration for consistency."""
        if not self.config:
            raise ValueError("Config not loaded")
        
        # Validate that all event providers exist as islands
        for event in self.config.events.values():
            if event.provider not in self.config.islands:
                raise ValueError(
                    f"Event '{event.name}' has unknown provider '{event.provider}'"
                )
            
            # Validate that provider actually produces this event
            provider_island = self.config.islands[event.provider]
            if event.name not in provider_island.produces:
                raise ValueError(
                    f"Island '{event.provider}' doesn't produce event '{event.name}'"
                )
            
            # Validate all consumers exist
            for consumer in event.consumers:
                if consumer not in self.config.islands:
                    raise ValueError(
                        f"Event '{event.name}' has unknown consumer '{consumer}'"
                    )
                
                # Validate that consumer actually consumes this event
                consumer_island = self.config.islands[consumer]
                if event.name not in consumer_island.consumes:
                    raise ValueError(
                        f"Island '{consumer}' doesn't consume event '{event.name}'"
                    )
        
        print(f"✓ Event bus configuration validated: {len(self.config.events)} events, "
              f"{len(self.config.islands)} islands")
    
    def get_event_priority(self, event_name: str) -> int:
        """Get priority for an event type."""
        if not self.config or event_name not in self.config.events:
            return 999  # Default low priority
        return self.config.events[event_name].priority
    
    def get_consumers(self, event_name: str) -> List[str]:
        """Get list of consumers for an event type."""
        if not self.config or event_name not in self.config.events:
            return []
        return self.config.events[event_name].consumers
    
    def is_synchronous(self, event_name: str) -> bool:
        """Check if event should be processed synchronously."""
        if not self.config:
            return True  # Default to synchronous
        return event_name in self.config.synchronous_events


# Global config instance
_config_loader: Optional[EventBusConfigLoader] = None


def get_event_bus_config() -> EventBusConfig:
    """Get the global event bus configuration."""
    global _config_loader
    if _config_loader is None:
        _config_loader = EventBusConfigLoader()
        _config_loader.load()
    return _config_loader.config


def reload_event_bus_config():
    """Reload the event bus configuration from disk."""
    global _config_loader
    _config_loader = EventBusConfigLoader()
    _config_loader.load()


if __name__ == "__main__":
    # Test loading the configuration
    loader = EventBusConfigLoader()
    config = loader.load()
    
    print("\n=== Event Bus Configuration ===")
    print(f"\nEvents ({len(config.events)}):")
    for event in config.events.values():
        print(f"  {event.name} (priority {event.priority})")
        print(f"    Provider: {event.provider}")
        print(f"    Consumers: {', '.join(event.consumers)}")
    
    print(f"\nIslands ({len(config.islands)}):")
    for island in config.islands.values():
        print(f"  {island.name}")
        print(f"    Produces: {', '.join(island.produces)}")
        print(f"    Consumes: {', '.join(island.consumes)}")
    
    print(f"\nQueue Type: {config.queue_type}")
    print(f"Retry Enabled: {config.retry_enabled}")
    print(f"Monitoring Enabled: {config.monitoring_enabled}")
