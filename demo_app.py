"""Comprehensive demo application for cjm-fasthtml-workers library.

This demo tests all library features:
- BaseJobManager with custom job types
- Worker processes with plugin system
- Job creation, monitoring, and completion
- Job cancellation and worker restart
- Hook methods (_on_job_completed, _validate_resources, _extract_model_identifier)
- Optional integrations (ResourceManager, EventBroadcaster, PluginRegistry)
- WorkerConfig and RestartPolicy
"""

import uuid
import time
import asyncio
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Callable, Iterator
from datetime import datetime

from fasthtml.common import *
from cjm_fasthtml_daisyui.core.resources import get_daisyui_headers
from cjm_fasthtml_daisyui.core.testing import create_theme_persistence_script

print("\n" + "="*70)
print("Initializing cjm-fasthtml-workers Demo")
print("="*70)

# Step 1: Import library components
print("\n[1/6] Importing library components...")
from cjm_fasthtml_workers.managers.base import BaseJobManager, BaseJob
from cjm_fasthtml_workers.core.worker import base_worker_process
from cjm_fasthtml_workers.core.config import WorkerConfig, RestartPolicy
from cjm_fasthtml_workers.core.protocol import (
    WorkerRequestType,
    WorkerResponseType,
    PluginManagerAdapter
)
from cjm_fasthtml_workers.core.adapters import create_simple_adapter, default_result_adapter
from cjm_fasthtml_workers.extensions.protocols import (
    PluginRegistryProtocol,
    ResourceManagerProtocol,
    EventBroadcasterProtocol
)
print("  ✓ All library components imported successfully")


# Step 2: Create a simple text processing plugin system for demo
print("\n[2/6] Setting up demo text processing plugin system...")

@dataclass
class TextProcessorPlugin:
    """Simple text processor plugin."""
    name: str
    version: str = "1.0.0"

    def process(self, text: str) -> Dict[str, Any]:
        """Process text based on plugin type."""
        if self.name == "word_counter":
            words = text.split()
            return {
                'text': f"Word count: {len(words)}",
                'metadata': {
                    'word_count': len(words),
                    'char_count': len(text),
                    'plugin': self.name
                }
            }
        elif self.name == "text_reverser":
            return {
                'text': text[::-1],
                'metadata': {
                    'original_length': len(text),
                    'plugin': self.name
                }
            }
        elif self.name == "uppercase":
            return {
                'text': text.upper(),
                'metadata': {
                    'plugin': self.name,
                    'transformation': 'uppercase'
                }
            }
        elif self.name == "slow_processor":
            # Simulates a slow operation (for testing cancellation)
            time.sleep(5)
            return {
                'text': f"Processed (slowly): {text}",
                'metadata': {'plugin': self.name, 'processing_time': '5s'}
            }
        else:
            return {'text': text, 'metadata': {'plugin': self.name}}


class TextProcessorManager:
    """Simple plugin manager for demo."""

    def __init__(self):
        self.plugins: Dict[str, TextProcessorPlugin] = {}
        self.configs: Dict[str, Dict[str, Any]] = {}

    def discover_plugins(self) -> List[Any]:
        """Discover available plugins."""
        return [
            type('PluginData', (), {'name': 'word_counter'}),
            type('PluginData', (), {'name': 'text_reverser'}),
            type('PluginData', (), {'name': 'uppercase'}),
            type('PluginData', (), {'name': 'slow_processor'}),
        ]

    def load_plugin(self, plugin_data: Any, config: Dict[str, Any]) -> None:
        """Load a plugin with configuration."""
        plugin_name = plugin_data.name
        self.plugins[plugin_name] = TextProcessorPlugin(name=plugin_name)
        self.configs[plugin_name] = config
        print(f"[Worker] Loaded plugin: {plugin_name}")

    def execute_plugin(self, plugin_name: str, **params) -> Any:
        """Execute a plugin."""
        if plugin_name not in self.plugins:
            raise ValueError(f"Plugin {plugin_name} not loaded")

        text = params.get('text', '')
        return self.plugins[plugin_name].process(text)

    def execute_plugin_stream(self, plugin_name: str, **params) -> Iterator[str]:
        """Execute plugin with streaming (not used in this demo)."""
        result = self.execute_plugin(plugin_name, **params)
        yield result.get('text', '')

    def reload_plugin(self, plugin_name: str, config: Optional[Dict[str, Any]] = None) -> None:
        """Reload a plugin with new configuration."""
        if config is None:
            # Unload
            if plugin_name in self.plugins:
                del self.plugins[plugin_name]
                del self.configs[plugin_name]
                print(f"[Worker] Unloaded plugin: {plugin_name}")
        else:
            # Reload
            self.plugins[plugin_name] = TextProcessorPlugin(name=plugin_name)
            self.configs[plugin_name] = config
            print(f"[Worker] Reloaded plugin: {plugin_name}")

    def unload_plugin(self, plugin_name: str) -> None:
        """Unload a plugin."""
        if plugin_name in self.plugins:
            del self.plugins[plugin_name]
            if plugin_name in self.configs:
                del self.configs[plugin_name]
            print(f"[Worker] Unloaded plugin: {plugin_name}")

    def check_streaming_support(self, plugin_name: str) -> bool:
        """Check if plugin supports streaming."""
        return False  # Not used in this demo


def text_processor_factory():
    """Factory for creating text processor manager in worker."""
    return TextProcessorManager()


print("  ✓ Text processing plugin system created")


# Step 3: Create mock integrations (Resource Manager, Event Broadcaster, Plugin Registry)
print("\n[3/6] Creating mock optional integrations...")

class MockResourceManager:
    """Mock resource manager for demo."""

    def __init__(self):
        self.workers: Dict[int, Dict[str, Any]] = {}

    def register_worker(self, pid: int, worker_type: str) -> None:
        """Register a worker."""
        self.workers[pid] = {
            'worker_type': worker_type,
            'status': 'idle',
            'registered_at': datetime.now().isoformat()
        }
        print(f"[ResourceMgr] Registered {worker_type} worker PID {pid}")

    def unregister_worker(self, pid: int) -> None:
        """Unregister a worker."""
        if pid in self.workers:
            del self.workers[pid]
            print(f"[ResourceMgr] Unregistered worker PID {pid}")

    def update_worker_state(
        self,
        pid: int,
        status: Optional[str] = None,
        job_id: Optional[str] = None,
        plugin_name: Optional[str] = None,
        plugin_id: Optional[str] = None,
        loaded_model: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update worker state."""
        if pid not in self.workers:
            return

        if status:
            self.workers[pid]['status'] = status
        if job_id is not None:
            self.workers[pid]['job_id'] = job_id
        if plugin_name is not None:
            self.workers[pid]['plugin_name'] = plugin_name
        if plugin_id is not None:
            self.workers[pid]['plugin_id'] = plugin_id
        if loaded_model is not None:
            self.workers[pid]['loaded_model'] = loaded_model

        # Log for demo visibility
        state_str = f"status={status}" if status else ""
        if job_id:
            state_str += f", job={job_id[:8]}"
        if loaded_model:
            state_str += f", model={loaded_model}"
        if state_str:
            print(f"[ResourceMgr] Worker {pid}: {state_str}")


class MockEventBroadcaster:
    """Mock event broadcaster for demo."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []

    async def broadcast(self, event_type: str, data: Dict[str, Any]) -> None:
        """Broadcast an event."""
        event = {
            'type': event_type,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        self.events.append(event)
        print(f"[EventBroadcaster] {event_type}: {data.get('job_id', 'N/A')[:8] if 'job_id' in data else 'N/A'}")


@dataclass
class MockPluginMetadata:
    """Mock plugin metadata."""
    name: str
    is_configured: bool = True

    def get_unique_id(self) -> str:
        return f"mock-{self.name}"


class MockPluginRegistry:
    """Mock plugin registry for demo."""

    def __init__(self):
        self.plugins = [
            MockPluginMetadata(name="word_counter"),
            MockPluginMetadata(name="text_reverser"),
            MockPluginMetadata(name="uppercase"),
            MockPluginMetadata(name="slow_processor"),
        ]
        self.configs = {
            "mock-word_counter": {"model_id": "word-counter-v1", "device": "cpu"},
            "mock-text_reverser": {"model_id": "reverser-v1", "device": "cpu"},
            "mock-uppercase": {"model_id": "uppercase-v1", "device": "cpu"},
            "mock-slow_processor": {"model_id": "slow-proc-v1", "device": "cpu"},
        }

    def get_plugins_by_category(self, category: Any) -> List[MockPluginMetadata]:
        """Get plugins by category."""
        return self.plugins

    def get_plugin(self, plugin_id: str) -> Optional[MockPluginMetadata]:
        """Get plugin by ID."""
        for plugin in self.plugins:
            if plugin.get_unique_id() == plugin_id:
                return plugin
        return None

    def load_plugin_config(self, plugin_id: str) -> Dict[str, Any]:
        """Load plugin configuration."""
        return self.configs.get(plugin_id, {})


# Create global instances
resource_manager = MockResourceManager()
event_broadcaster = MockEventBroadcaster()
plugin_registry = MockPluginRegistry()

print("  ✓ Resource manager created")
print("  ✓ Event broadcaster created")
print("  ✓ Plugin registry created with 4 plugins")


# Step 4: Create custom job manager
print("\n[4/6] Creating custom text processing job manager...")

@dataclass
class TextProcessingJob(BaseJob):
    """Job for text processing tasks."""
    text: str = ""
    plugin_name: str = ""


# Worker entry point
def text_processor_worker(request_queue, result_queue, response_queue):
    """Worker process for text processing."""
    base_worker_process(
        request_queue=request_queue,
        result_queue=result_queue,
        response_queue=response_queue,
        plugin_manager_factory=text_processor_factory,
        result_adapter=default_result_adapter,
        supports_streaming=False
    )


class TextProcessingManager(BaseJobManager[TextProcessingJob]):
    """Manager for text processing jobs with custom hooks."""

    def __init__(self, worker_config: Optional[WorkerConfig] = None):
        super().__init__(
            worker_type="text_processing",
            category="processing",  # Mock category
            supports_streaming=False,
            worker_config=worker_config,
            plugin_registry=plugin_registry,
            resource_manager=resource_manager,
            event_broadcaster=event_broadcaster
        )

        # Track completed jobs for demo
        self.completed_count = 0

    def create_job(self, plugin_id: str, **kwargs) -> TextProcessingJob:
        """Create a text processing job."""
        plugin_name = self.get_plugin_name(plugin_id)
        return TextProcessingJob(
            id=str(uuid.uuid4()),
            plugin_id=plugin_id,
            text=kwargs.get('text', ''),
            plugin_name=plugin_name or "unknown"
        )

    def get_worker_entry_point(self) -> Callable:
        """Get worker entry point."""
        return text_processor_worker

    def prepare_execute_request(self, job: TextProcessingJob) -> Dict[str, Any]:
        """Prepare execution request."""
        return {'text': job.text}

    def extract_job_result(self, job: TextProcessingJob, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract job result."""
        return result_data

    # Hook: Extract model identifier
    def _extract_model_identifier(self, config: Dict[str, Any]) -> str:
        """Custom model ID extraction."""
        # Demo: try model_id, then model, then fallback
        return config.get('model_id', config.get('model', 'unknown-model'))

    # Hook: Validate resources before job
    async def _validate_resources(self, plugin_id: str, plugin_config: Dict[str, Any]) -> Optional[str]:
        """Validate resources before starting job."""
        # Demo: Simple validation - check if another job is running
        running_jobs = [j for j in self.jobs.values() if j.status == 'running']
        if len(running_jobs) > 0:
            # Allow it anyway for demo, but log warning
            print(f"[Validation] Warning: Job already running, but proceeding anyway")
        return None  # No error, allow job to proceed

    # Hook: Custom completion handler
    def _on_job_completed(self, job_id: str) -> None:
        """Handle job completion."""
        self.completed_count += 1
        job = self.get_job(job_id)
        result = self.get_job_result(job_id)

        print(f"[Completion Hook] Job {job_id[:8]} completed!")
        print(f"[Completion Hook] Plugin: {job.plugin_name}")
        print(f"[Completion Hook] Total completed: {self.completed_count}")

        # Could save to file, database, send notification, etc.
        # For demo, just print


# Create manager with custom config
demo_config = WorkerConfig(
    restart_policy=RestartPolicy.ON_CANCELLATION,
    max_restart_attempts=3,
    result_queue_size=50
)

job_manager = TextProcessingManager(worker_config=demo_config)

print("  ✓ Text processing job manager created")
print(f"  ✓ Restart policy: {demo_config.restart_policy.value}")
print(f"  ✓ Max restart attempts: {demo_config.max_restart_attempts}")


# Step 5: Set up FastHTML app
print("\n[5/6] Setting up FastHTML application...")


def main():
    """Main entry point - creates the demo app."""

    # Create the FastHTML app
    app, rt = fast_app(
        pico=False,
        hdrs=[
            *get_daisyui_headers(),
            create_theme_persistence_script(),
            Script("""
            // Auto-refresh job list every 2 seconds
            setInterval(() => {
                const jobList = document.getElementById('job-list');
                if (jobList) {
                    htmx.trigger('#job-list', 'refresh');
                }
            }, 2000);
            """)
        ],
        title="FastHTML Workers Demo",
        htmlkw={'data-theme': 'light'}
    )

    from cjm_fasthtml_tailwind.utilities.spacing import p, m
    from cjm_fasthtml_tailwind.utilities.sizing import container, max_w, w
    from cjm_fasthtml_tailwind.utilities.typography import font_size, font_weight, text_align
    from cjm_fasthtml_tailwind.core.base import combine_classes
    from cjm_fasthtml_daisyui.components.actions.button import btn, btn_colors, btn_sizes, btn_styles
    from cjm_fasthtml_daisyui.components.data_display.badge import badge, badge_colors, badge_styles
    from cjm_fasthtml_daisyui.components.data_input.text_input import text_input, text_input_sizes
    from cjm_fasthtml_daisyui.components.data_input.textarea import textarea, textarea_sizes
    from cjm_fasthtml_daisyui.components.data_input.select import select, select_sizes

    @rt("/")
    def get():
        """Homepage with feature showcase."""
        return Main(
            Div(
                H1("cjm-fasthtml-workers Demo",
                   cls=combine_classes(font_size._4xl, font_weight.bold, m.b(4))),

                P("Comprehensive demonstration of background job processing with worker processes:",
                  cls=combine_classes(font_size.lg, m.b(6))),

                # Feature list
                Div(
                    Div(
                        Span("✓", cls=combine_classes(font_size._2xl, m.r(3))),
                        Span("BaseJobManager with custom job types"),
                        cls=combine_classes(m.b(3))
                    ),
                    Div(
                        Span("✓", cls=combine_classes(font_size._2xl, m.r(3))),
                        Span("Isolated worker processes with plugin system"),
                        cls=combine_classes(m.b(3))
                    ),
                    Div(
                        Span("✓", cls=combine_classes(font_size._2xl, m.r(3))),
                        Span("Job creation, monitoring, and completion tracking"),
                        cls=combine_classes(m.b(3))
                    ),
                    Div(
                        Span("✓", cls=combine_classes(font_size._2xl, m.r(3))),
                        Span("Job cancellation with worker restart"),
                        cls=combine_classes(m.b(3))
                    ),
                    Div(
                        Span("✓", cls=combine_classes(font_size._2xl, m.r(3))),
                        Span("Hook methods for customization"),
                        cls=combine_classes(m.b(3))
                    ),
                    Div(
                        Span("✓", cls=combine_classes(font_size._2xl, m.r(3))),
                        Span("Optional integrations (ResourceManager, EventBroadcaster, PluginRegistry)"),
                        cls=combine_classes(m.b(8))
                    ),
                    cls=combine_classes(text_align.left, m.b(8))
                ),

                # Statistics
                Div(
                    Span(
                        Span(f"{len(plugin_registry.plugins)}", cls=str(font_weight.bold)),
                        " Plugins Available",
                        cls=combine_classes(badge, badge_colors.info, m.r(2))
                    ),
                    Span(
                        Span(f"{job_manager.completed_count}", cls=str(font_weight.bold)),
                        " Jobs Completed",
                        cls=combine_classes(badge, badge_colors.success, m.r(2))
                    ),
                    Span(
                        Span(f"{len([j for j in job_manager.jobs.values() if j.status == 'running'])}", cls=str(font_weight.bold)),
                        " Jobs Running",
                        cls=combine_classes(badge, badge_colors.warning)
                    ),
                    cls=combine_classes(m.b(8))
                ),

                A(
                    "Try the Job Processor",
                    href="/processor",
                    cls=combine_classes(btn, btn_colors.primary, btn_sizes.lg)
                ),

                cls=combine_classes(
                    container,
                    max_w._4xl,
                    m.x.auto,
                    p(8),
                    text_align.center
                )
            )
        )

    @rt("/processor")
    def get():
        """Interactive job processor page."""
        return Main(
            Div(
                H1("Text Processing Job Manager",
                   cls=combine_classes(font_size._3xl, font_weight.bold, m.b(6))),

                # Job creation form
                Div(
                    H2("Create New Job", cls=combine_classes(font_size._2xl, font_weight.bold, m.b(4))),
                    Form(
                        Div(
                            Label("Select Processor:", cls=combine_classes(font_weight.bold, m.b(2))),
                            Select(
                                Option("Word Counter", value="mock-word_counter"),
                                Option("Text Reverser", value="mock-text_reverser"),
                                Option("Uppercase", value="mock-uppercase"),
                                Option("Slow Processor (5s - test cancellation)", value="mock-slow_processor"),
                                name="plugin_id",
                                id="plugin-select",
                                cls=combine_classes(select, select_sizes.md, w.full, m.b(4))
                            ),
                        ),
                        Div(
                            Label("Enter Text:", cls=combine_classes(font_weight.bold, m.b(2))),
                            Textarea(
                                "Hello, FastHTML Workers!",
                                name="text",
                                id="text-input",
                                rows=4,
                                cls=combine_classes(textarea, textarea_sizes.md, w.full, m.b(4))
                            ),
                        ),
                        Button(
                            "Process Text",
                            type="submit",
                            cls=combine_classes(btn, btn_colors.primary, btn_sizes.md)
                        ),
                        hx_post="/jobs/create",
                        hx_target="#job-list",
                        hx_swap="outerHTML"
                    ),
                    cls=combine_classes(m.b(8), p(6), "bg-base-200", "rounded-lg")
                ),

                # Job list
                Div(
                    id="job-list",
                    hx_get="/jobs/list",
                    hx_trigger="load, refresh from:body",
                    hx_swap="outerHTML",
                    cls=combine_classes(m.b(4))
                ),

                # Back to home
                A(
                    "← Back to Home",
                    href="/",
                    cls=combine_classes(btn, btn_styles.ghost, m.t(4))
                ),

                cls=combine_classes(
                    container,
                    max_w._4xl,
                    m.x.auto,
                    p(8)
                )
            )
        )

    @rt("/jobs/create")
    async def post(plugin_id: str, text: str):
        """Create a new job."""
        try:
            job = await job_manager.start_job(plugin_id=plugin_id, text=text)
            print(f"\n[API] Created job {job.id[:8]} with plugin {job.plugin_name}")
        except Exception as e:
            print(f"\n[API] Error creating job: {e}")

        return await jobs_list()

    @rt("/jobs/list")
    async def jobs_list():
        """List all jobs."""
        from cjm_fasthtml_daisyui.components.data_display.table import table, table_modifiers

        jobs = job_manager.get_all_jobs()
        jobs.sort(key=lambda j: j.created_at, reverse=True)

        if not jobs:
            return Div(
                H2("Jobs", cls=combine_classes(font_size._2xl, font_weight.bold, m.b(4))),
                P("No jobs yet. Create one above!",
                  cls=combine_classes("text-gray-500", "italic")),
                id="job-list"
            )

        # Create table rows
        rows = []
        for job in jobs[:10]:  # Show last 10 jobs
            status_badge = badge_colors.warning
            if job.status == 'completed':
                status_badge = badge_colors.success
            elif job.status == 'failed':
                status_badge = badge_colors.error
            elif job.status == 'cancelled':
                status_badge = badge_styles.ghost

            result_text = ""
            if job.status == 'completed' and job.result:
                result_text = job.result.get('text', 'N/A')[:50]
            elif job.status == 'failed':
                result_text = job.error[:50] if job.error else 'Error'
            elif job.status == 'running':
                result_text = "Processing..."

            cancel_btn = ""
            if job.status == 'running':
                cancel_btn = Button(
                    "Cancel",
                    hx_post=f"/jobs/{job.id}/cancel",
                    hx_target="#job-list",
                    hx_swap="outerHTML",
                    cls=combine_classes(btn, btn_colors.error, btn_sizes.xs)
                )

            rows.append(
                Tr(
                    Td(job.id[:8] + "...", cls="font-mono text-sm"),
                    Td(job.plugin_name),
                    Td(Span(job.status, cls=combine_classes(badge, status_badge))),
                    Td(result_text, cls="max-w-xs truncate"),
                    Td(cancel_btn)
                )
            )

        return Div(
            H2("Jobs", cls=combine_classes(font_size._2xl, font_weight.bold, m.b(4))),
            Div(
                Table(
                    Thead(
                        Tr(
                            Th("Job ID"),
                            Th("Plugin"),
                            Th("Status"),
                            Th("Result"),
                            Th("Actions")
                        )
                    ),
                    Tbody(*rows),
                    cls=combine_classes(table, table_modifiers.zebra, w.full)
                ),
                cls="overflow-x-auto"
            ),
            id="job-list"
        )

    @rt("/jobs/{job_id}/cancel")
    async def post(job_id: str):
        """Cancel a job."""
        success = await job_manager.cancel_job(job_id)
        if success:
            print(f"\n[API] Cancelled job {job_id[:8]}")
        else:
            print(f"\n[API] Failed to cancel job {job_id[:8]}")

        return await jobs_list()

    print("  ✓ FastHTML application configured")
    print("  ✓ Routes registered")

    print("\n" + "="*70)
    print("Demo App Ready!")
    print("="*70)
    print("\n📦 Worker System Configuration:")
    print(f"  • Worker type: {job_manager.worker_type}")
    print(f"  • Restart policy: {job_manager.worker_config.restart_policy.value}")
    print(f"  • Max restart attempts: {job_manager.worker_config.max_restart_attempts}")
    print(f"  • Resource manager: {'✓ Enabled' if job_manager.resource_manager else '✗ Disabled'}")
    print(f"  • Event broadcaster: {'✓ Enabled' if job_manager.event_broadcaster else '✗ Disabled'}")
    print(f"  • Plugin registry: {'✓ Enabled' if job_manager.plugin_registry else '✗ Disabled'}")

    print("\n🔌 Available Text Processors:")
    for plugin in plugin_registry.plugins:
        print(f"  • {plugin.name}")

    print("\n🎣 Custom Hooks:")
    print("  • _extract_model_identifier() - Custom model ID extraction")
    print("  • _validate_resources() - Pre-job validation")
    print("  • _on_job_completed() - Post-completion handler")

    print("="*70 + "\n")

    return app


print("\n[6/6] Application ready to start!")


if __name__ == "__main__":
    import uvicorn
    import webbrowser
    import threading

    # Call main to initialize everything and get the app
    app = main()

    def open_browser(url):
        print(f"🌐 Opening browser at {url}")
        webbrowser.open(url)

    port = 5030
    host = "0.0.0.0"
    display_host = 'localhost' if host in ['0.0.0.0', '127.0.0.1'] else host

    print(f"🚀 Server: http://{display_host}:{port}")
    print("\n📍 Available routes:")
    print(f"  http://{display_host}:{port}/           - Homepage with feature list")
    print(f"  http://{display_host}:{port}/processor  - Interactive job processor")
    print("\n" + "="*70 + "\n")

    # Open browser after a short delay
    timer = threading.Timer(1.5, lambda: open_browser(f"http://localhost:{port}"))
    timer.daemon = True
    timer.start()

    # Start server
    uvicorn.run(app, host=host, port=port)
