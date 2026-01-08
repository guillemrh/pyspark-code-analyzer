from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource

def setup_tracing(service_name: str):
    # 1. Define 'Resource' to identify your service in the Jaeger UI
    resource = Resource.create(
        {
            "service.name": service_name,
        }
    )

    # 2. The TracerProvider is the central 'hub' that manages tracing logic
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    # 3. Configure the Exporter: Tells OTel where to send the data (the Jaeger backend)
    jaeger_exporter = JaegerExporter(
        agent_host_name="jaeger", # The hostname of your Jaeger instance
        agent_port=6831,          # The default port for the Jaeger agent (UDP)
    )

    # 4. Use a BatchSpanProcessor to send spans in groups for better performance
    provider.add_span_processor(
        BatchSpanProcessor(jaeger_exporter)
    )

    # 5. Return a tracer instance to start creating spans in your app logic
    return trace.get_tracer(service_name)